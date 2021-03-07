package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

public class TwitterReduceSideJoinWithMaxFilter {

  private static long MAX_FILTER_VALUE = 50000;

  enum numberOfTriangles {
    TRIANGLE_COUNT;
  }

  public static class Path2CountJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      String[] followerFollowing = value.toString().split(",");
      if (followerFollowing.length == 2 && followerFollowing[1] == null) {
        return;
      }

      if (isWithinFilter(followerFollowing)) {

        // Emit key (Y, AX,Y)
        outkey.set(followerFollowing[1]);
        // Flag this record for the reducer and then output
        outvalue.set("A" + value.toString());
        context.write(outkey, outvalue);

        // Emitting second value for self join,        // Emit key (X, BX,Y)
        outkey.set(followerFollowing[0]);
        // Flag this record for the reducer and then output
        outvalue.set("B" + value.toString());
        context.write(outkey, outvalue);
      }
    }
  }

  private static boolean isWithinFilter(String[] followerFollowing) { //Max filter to filter out edges with values
    //greater than defined MAX_FILTER

    long follower = Integer.valueOf(followerFollowing[0]);
    long following = Integer.valueOf(followerFollowing[1]);

    return follower < MAX_FILTER_VALUE && following < MAX_FILTER_VALUE;
  }

  public static class Path2CountJoinReducer extends Reducer<Text, Text, NullWritable, Text> {

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    @Override
    public void setup(Context context) {
      // Get the type of join from our configuration
      joinType = context.getConfiguration().get("join.type");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      // Clear our lists
      listA.clear();
      listB.clear();

      // iterate through all our values, binning each record based on what
      // it was tagged with and remove the tag
      for (Text t : values) {
        if (t.charAt(0) == 'A') {  //Incoming edge
          listA.add(new Text(t.toString().substring(1)));
        } else if (t.charAt(0) == 'B') { //Outgoing edge
          listB.add(new Text(t.toString().substring(1)));
        }
      }
      // Execute our join logic now that the lists are filled
      executeEquiJoinLogic(key, context);
    }

    private void executeEquiJoinLogic(Text key, Context context) throws IOException,
            InterruptedException {
      final IntWritable result = new IntWritable();
      if (joinType.equalsIgnoreCase("inner")) {
        // If both lists are not empty, join A with B
        if (!listA.isEmpty() && !listB.isEmpty()) {
          for (Text textA : listA) {
            for (Text textB : listB) {
              Text modifiedText = new Text();
              modifiedText.set(textA.toString() + "," + textB.toString().split(",")[1]);
              context.write(NullWritable.get(), modifiedText); // EMIT (X,Y,Z) and write as intermediate output
              //Output as (X,Y,Z) pair.}
            }
          }
        }
      }
    }
  }

  public static class Path3CountJoinMapper extends Mapper<Object, Text, Text, Text> {

    //Input to this mapper is (X,Y,Z)
    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      String[] followerFollowing = value.toString().split(",");

      if (followerFollowing.length != 3 && followerFollowing[1] == null) {
        return;
      }
      // From (X,Y,Z) Now we emit , (X,Z) as the Key
      outkey.set(followerFollowing[0] + "," + followerFollowing[2]);
      outvalue.set("P2");
      context.write(outkey, outvalue);
    }
  }


  public static class Path3CountJoinMapper2 extends Mapper<Object, Text, Text, Text> {

    //Input to this mapper is edges CSV i.e (Z,X)
    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      String[] followerFollowing = value.toString().split(",");
      if (followerFollowing.length == 2 && followerFollowing[1] == null) {
        return;
      }

      if (isWithinFilter(followerFollowing)) {
        //Input to this mapper is edge (Z,X), to match with the earlier key we need to emit (X,Z)
        outkey.set(followerFollowing[1] + "," + followerFollowing[0]);
        // Flag this record for the reducer and then output
        outvalue.set("ZXEdge");
        context.write(outkey, outvalue);
      }
    }
  }


  public static class Path3CountReducer extends Reducer<Text, Text, Text, Text> {

    int globalSum = 0;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
      int countXZPaths = 0;
      int countZXPaths = 0;

      for (Text t : values) {
        if (t.toString().equals("P2")) countXZPaths++;  //Edges from path2Count
        else if (t.toString().equals("ZXEdge")) countZXPaths++;  //ZX edges
      }
      globalSum += countXZPaths * countZXPaths;  //The total sum is the cross product of both as
      //They join on same key
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      context.getCounter(numberOfTriangles.TRIANGLE_COUNT).increment(globalSum);
    }
  }


  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err
              .println("Usage: ReduceSideJoin <twitter data> <out>");
      System.exit(1);
    }

    String joinType = "inner";
    final Job job = Job.getInstance(conf, "Reduce Side Join");

    // Configure the join type
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(TwitterReduceSideJoinWithMaxFilter.class);

    // Use multiple inputs to set which input uses what mapper
    // This will keep parsing of each data set separate from a logical
    // standpoint
    // However, this version of Hadoop has not upgraded MultipleInputs
    // to the mapreduce package, so we have to use the deprecated API.
    // Future releases have this in the "mapreduce" package.
    MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
            TextInputFormat.class, Path2CountJoinMapper.class);
    job.setReducerClass(Path2CountJoinReducer.class);

    FileOutputFormat.setOutputPath(job, new Path("intermediateOutput"));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.waitForCompletion(true);

    // New Job configuration
    Job job2 = Job.getInstance(conf, "Reduce Side Join: Job 2");
    job2.setJarByClass(TwitterReduceSideJoinWithMaxFilter.class);

    MultipleInputs.addInputPath(job2, new Path("intermediateOutput"),
            TextInputFormat.class, Path3CountJoinMapper.class);
    MultipleInputs.addInputPath(job2, new Path(otherArgs[0]),
            TextInputFormat.class, Path3CountJoinMapper2.class);

    job2.setReducerClass(Path3CountReducer.class);

    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    int val = job2.waitForCompletion(true) ? 0 : 3;
    Counters triangleCounter = job2.getCounters();
    Counter totalTriangleCount = triangleCounter.findCounter(numberOfTriangles.TRIANGLE_COUNT);
    System.out.println("Total number of triangles: " + totalTriangleCount.getValue() / 3);
    System.exit(val);
  }
}
