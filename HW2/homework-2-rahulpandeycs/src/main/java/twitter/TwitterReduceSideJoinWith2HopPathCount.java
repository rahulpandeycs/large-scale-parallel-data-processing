package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;

public class TwitterReduceSideJoinWith2HopPathCount {

  private static long MAX_FILTER_VALUE = 100000;

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

      // The foreign join key is the user ID
      outkey.set(followerFollowing[1]);

      // Flag this record for the reducer and then output
      outvalue.set("A" + value.toString());
      //   System.out.println("Emitted, key: " + outkey.toString() + " row Value: " + outvalue.toString());
      context.write(outkey, outvalue);

      // The foreign join key is the user ID
      outkey.set(followerFollowing[0]);
      // Flag this record for the reducer and then output
      outvalue.set("B" + value.toString());
      context.write(outkey, outvalue);
    }
  }

  public static class UserJoinReducer extends Reducer<Text, Text, Text, IntWritable> {

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
      // it was tagged with
      // make sure to remove the tag!
      for (Text t : values) {
        if (t.charAt(0) == 'A') {
          listA.add(new Text(t.toString().substring(1)));
        } else if (t.charAt(0) == 'B') {
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
          result.set(listA.size() * listB.size());
//            System.out.println("Writing to context in reduce" + A + ":::" + B);
          context.write(key, result);
        }
      }
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
    job.setJarByClass(TwitterReduceSideJoinWith2HopPathCount.class);

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

    System.exit(job.waitForCompletion(true) ? 0 : 3);
  }
}
