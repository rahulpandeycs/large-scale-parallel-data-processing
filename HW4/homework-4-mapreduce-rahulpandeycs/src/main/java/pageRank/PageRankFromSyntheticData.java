package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

class PageNode implements Writable {

  public int id = -1;
  public double pageRank = -1;
  public String adjList = "";

  PageNode() {
  }

  PageNode(int id, double pageRank, String adjList) {
    this.id = id;
    this.pageRank = pageRank;
    this.adjList = adjList;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(id);
    dataOutput.writeUTF(adjList);
    dataOutput.writeDouble(pageRank);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    id = dataInput.readInt();
    adjList = dataInput.readUTF();
    pageRank = dataInput.readDouble();
  }

  @Override
  public String toString() {
    return "" + this.adjList + "," + String.format("%2.12f", this.pageRank);
  }
}

enum PageRankGlobalCounter {
  PAGE_RANK_SUM,
  DELTA_SUM
}

public class PageRankFromSyntheticData extends Configured implements Tool {
  private static final Logger logger = LogManager.getLogger(PageRankFromSyntheticData.class);
  public final static int k = 1000;

  //Custom class: PageRank, id, ListOfAdj
  public static class PageRankMapper extends Mapper<Object, Text, Text, PageNode> {
    private int totalNumberOfPages;
    private double delta;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      totalNumberOfPages = Integer.parseInt(context.getConfiguration().get("totalNumberOfPages"));
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final String[] graphAndPageRank = value.toString().split(",");

      if (graphAndPageRank.length != 2)
        return;

      //For input vertex X in row: X Y
      final String[] inputSplit = graphAndPageRank[0].split(" ");

//      PageNode pageNode = new PageNode(Integer.parseInt(inputSplit[0]), 0.0, adjList.trim());

      //For synthetic data
      double calculatedPageRank = Double.parseDouble(graphAndPageRank[1]) + (0.85) * delta / totalNumberOfPages;

      PageNode pageNode = new PageNode(Integer.parseInt(inputSplit[0]), calculatedPageRank, (inputSplit.length > 1 ? inputSplit[1] : ""));
      int adjSize = inputSplit.length - 1;

      context.write(new Text(String.valueOf(pageNode.id)), pageNode);

      if (adjSize > 0) {
        double pgRnkToDistribute = ((calculatedPageRank) / (double) (adjSize));
        for (int i = 1; i < inputSplit.length; i++) {
          context.write(new Text(inputSplit[i]), new PageNode(-1, pgRnkToDistribute, ""));
        }
      } //else {  //Handle dangling page
//        context.getCounter(pageRankGlobalCounter.danglingMass).increment((long) pageNode.pageRank);
//      }
    }
  }

  public static class PageRankReducer extends Reducer<Text, PageNode, Text, PageNode> {

    private int totalNumberOfPages;

    @Override
    protected void setup(Reducer<Text, PageNode, Text, PageNode>.Context context) throws IOException, InterruptedException {
      totalNumberOfPages = Integer.parseInt(context.getConfiguration().get("totalNumberOfPages"));
    }

    @Override
    public void reduce(final Text key, final Iterable<PageNode> values, final Context context) throws IOException, InterruptedException {
      double sum = 0.0;
      PageNode newNode = new PageNode();

      for (PageNode node : values) {
        if (node.id != -1) { //The vertex was found
          newNode = new PageNode(node.id, node.pageRank, node.adjList);
        } else {
          sum += node.pageRank;
        }
      }
      //sum = sum / 2.0;
      if (Integer.parseInt(key.toString()) != 0) //If not the dummyNode and not null
        newNode.pageRank = (0.15) * (1.0) / (totalNumberOfPages) + (0.85 * sum);
      else
        newNode.pageRank = 0.0;

      // emit(id adjList, pageRank)
      context.write(key, newNode);

      //If dummyNode, pass pageRank to next node
      if (newNode.id == 0) {
        context.getCounter(PageRankGlobalCounter.DELTA_SUM).setValue((long) (sum * (10000000)));
      }
    }
  }


  public static class LastJobMapper extends Mapper<Object, Text, Text, PageNode> {
//    private final Text word = new Text();
    private int totalNumberOfPages;
    private double delta;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      totalNumberOfPages = Integer.parseInt(context.getConfiguration().get("totalNumberOfPages"));
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

      final String[] graphAndPageRank = value.toString().split(",");
      if (graphAndPageRank.length != 2)
        return;

      //For input vertex X in row: X Y
      final String[] inputSplit = graphAndPageRank[0].split(" ");
      PageNode pageNode = new PageNode(Integer.parseInt(inputSplit[0]), 0.0, (inputSplit.length > 1 ? inputSplit[1] : ""));

      if (pageNode.id != 0) {
        pageNode.pageRank = Double.parseDouble(graphAndPageRank[1]) + ((0.85 * delta) / (totalNumberOfPages));
      }

      context.write(new Text(String.valueOf(pageNode.id)), pageNode);

      context.getCounter(PageRankGlobalCounter.PAGE_RANK_SUM).increment((long) (pageNode.pageRank * (10000000)));
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    Configuration conf = getConf();
    int iterationNum = 1;
    double delta = 0.0;

    //Job 0
    Job job = Job.getInstance(conf, "PageRank MR: " + iterationNum);
    job.setJarByClass(PageRankFromSyntheticData.class);
    Configuration jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");

    jobConf.set("delta", "" + delta);
    jobConf.set("totalNumberOfPages", "" + k * k);
    job.setMapperClass(PageRankMapper.class);
    job.setReducerClass(PageRankReducer.class);
    job.setInputFormatClass(NLineInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PageNode.class);

    NLineInputFormat.addInputPath(job, new Path(args[0]));
    jobConf.setInt("mapreduce.input.lineinputformat.linespermap", k * k / 20);

    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/Job" + iterationNum));
    job.waitForCompletion(true);

    iterationNum++;

    //Next set of Jobs
    while (iterationNum <= 10) {
      double currDelta = (double) job.getCounters().findCounter(PageRankGlobalCounter.DELTA_SUM).getValue() / (10000000);

      conf = getConf();
      job = Job.getInstance(conf, "PageRankMR: " + iterationNum);
      job.setJarByClass(PageRankFromSyntheticData.class);
      jobConf = job.getConfiguration();
      jobConf.set("mapreduce.output.textoutputformat.separator", " ");

      job.setMapperClass(PageRankMapper.class);
      job.setReducerClass(PageRankReducer.class);
      job.setInputFormatClass(NLineInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(PageNode.class);

      //Setting global accessible data for job
      jobConf.set("delta", "" + currDelta);
      jobConf.set("totalNumberOfPages", "" + k * k);

      NLineInputFormat.addInputPath(job, new Path(args[1] + "/Job" + (iterationNum - 1)));
      jobConf.setInt("mapreduce.input.lineinputformat.linespermap", k * k / 20);

      FileOutputFormat.setOutputPath(job, new Path(args[1] + "/Job" + iterationNum));

      job.waitForCompletion(true);

      iterationNum++;
    }

    //Last job to add delta values of last iteration via @LastJobMapper
    double currDelta = (double) job.getCounters().findCounter(PageRankGlobalCounter.DELTA_SUM).getValue() / (10000000.0);
    conf = getConf();
    job = Job.getInstance(conf, "PageRank MR: " + iterationNum);
    job.setJarByClass(PageRankFromSyntheticData.class);
    jobConf = job.getConfiguration();
    jobConf.set("mapreduce.output.textoutputformat.separator", " ");

    job.setMapperClass(LastJobMapper.class);
    job.setNumReduceTasks(0); // No reduce, map only job

    job.setInputFormatClass(NLineInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PageNode.class);
    //Setting global accessible data for job
    jobConf.set("delta", "" + currDelta);
    jobConf.set("totalNumberOfPages", "" + k * k);


    NLineInputFormat.addInputPath(job, new Path(args[1] + "/Job" + (iterationNum - 1)));
    jobConf.setInt("mapreduce.input.lineinputformat.linespermap", k * k / 20);

    FileOutputFormat.setOutputPath(job, new Path(args[1] + "/Job" + "LastJobOutput"));

    int jobStatus = job.waitForCompletion(true) ? 0 : 1;
    System.out.println("The sum of all pages pageRank is: " + (double) (job.getCounters().findCounter(PageRankGlobalCounter.PAGE_RANK_SUM).getValue()) / (10000000));
    return jobStatus;
  }

  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Two arguments required:\n<input-dir> <output-dir>");
    }

    try {
      ToolRunner.run(new PageRankFromSyntheticData(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }
}