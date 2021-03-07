package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class CountAnalysisTwitterReduceSideJoinWith2HopPathCount {

  enum triangleCount {
    PATH2_COUNT;
  }

  public static class FollowingToFollowerJoinMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      String[] followerFollowing = value.toString().split(",");
      if (followerFollowing.length == 2 && followerFollowing[1] == null) {
        return;
      }

      // The foreign join key is the userY
      outkey.set(followerFollowing[1]);

      // Flag this record for the reducer to mark incoming edge and then output
      outvalue.set("A");
      context.write(outkey, outvalue);

      // The foreign join key userX
      outkey.set(followerFollowing[0]);
      // Flag this record for the reducer to mark outgoing edge and then output
      outvalue.set("B");
      context.write(outkey, outvalue);
    }
  }

  public static class UserJoinReducer extends Reducer<Text, Text, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      // Clear our lists
      long edgesIncoming = 0;
      long edgesOutgoing = 0;

      // iterate through all our values, binning each record based on what
      // it was tagged with
      // Remove the tag!
      for (Text t : values) {
        if (t.charAt(0) == 'A') { //incoming edges
          edgesIncoming++;
        } else if (t.charAt(0) == 'B') { //outgoing edge
          edgesOutgoing++;
        }
      }

      // Now the total count is the multiplication of both incoming and outgoing edges.
      long currentCount = edgesOutgoing * edgesIncoming;
      context.getCounter(triangleCount.PATH2_COUNT).increment(currentCount);
      context.write(key, new LongWritable(currentCount));
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
    Job job = new Job(conf, "Reduce Side Join");

    // Configure the join type
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(CountAnalysisTwitterReduceSideJoinWith2HopPathCount.class);

    // Use multiple inputs to set which input uses what mapper
    // This will keep parsing of each data set separate from a logical
    // standpoint
    // However, this version of Hadoop has not upgraded MultipleInputs
    // to the mapreduce package, so we have to use the deprecated API.
    // Future releases have this in the "mapreduce" package.
    MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
            TextInputFormat.class, FollowingToFollowerJoinMapper.class);

    job.setReducerClass(UserJoinReducer.class);

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    int val = job.waitForCompletion(true) ? 0 : 3;
    Counters triangleCounter = job.getCounters();
    Counter totalTriangleCount = triangleCounter.findCounter(triangleCount.PATH2_COUNT);
    System.out.println("Path2 Count: " + totalTriangleCount.getValue());
    System.exit(val);
  }
}
