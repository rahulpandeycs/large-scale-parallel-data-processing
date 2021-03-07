package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TwitterReplicatedJoin {

  private static long MAX_FILTER_VALUE = 130000;

  enum numberOfTriangles {  //Global counter defined to keep track of traingle count
    TRIANGLE_COUNT;
  }

  public static class ReplicatedJoinMapper extends
          Mapper<Object, Text, NullWritable, Text> {

    private HashMap<String, Set<String>> pathXYZMap = new HashMap<>();
    private Text outvalue = new Text();

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
      try {
        URI[] files = context.getCacheFiles();
        if (files == null || files.length == 0) {
          throw new RuntimeException(
                  "User information is not set in DistributedCache");
        }

        FileSystem fs = FileSystem.get(context.getConfiguration());
        // Read all files in the DistributedCache
        for (URI p : files) {
          //BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(new Path(p.toString()))));
          BufferedReader rdr = new BufferedReader(new FileReader("filelabel"));
          String line;
          // For each record in the user file
          while ((line = rdr.readLine()) != null) {
            String[] following = line.split(",");
            if (!isWithinFilter(following)) continue;
            if (following.length == 2 && following[1] != null) {
              // For each user X, put the list of users it is following
              if (!pathXYZMap.containsKey(following[0])) {
                pathXYZMap.put(following[0], new HashSet<>());
              }
              Set<String> usersSet = pathXYZMap.get(following[0]);
              usersSet.add(following[1]);
              pathXYZMap.put(following[0], usersSet);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

      String[] followerFollowing = value.toString().split(",");
      if (!isWithinFilter(followerFollowing)) return;

      if (followerFollowing[0] == null) {
        return;
      }

      // Check for each input user if path (X->Y), (Y->Z), (Z->X)
      if (pathXYZMap.containsKey(followerFollowing[1])) { //Get list of users Y follows
        Set<String> zUsersYFollows = pathXYZMap.get(followerFollowing[1]);
        for (String zUser : zUsersYFollows) {
          if(pathXYZMap.containsKey(zUser)){
            Set<String> xUsersZFollow = pathXYZMap.get(zUser); //List of X users that Z follows
            if(xUsersZFollow.contains(followerFollowing[0])) // there is match means the triangle is complete
              context.getCounter(numberOfTriangles.TRIANGLE_COUNT).increment(1);
          }
        }
      }
    }
  }

  private static boolean isWithinFilter(String[] followerFollowing) {//Max filter to filter out edges with values
    //greater than defined MAX_FILTER

    long follower = Integer.valueOf(followerFollowing[0]);
    long following = Integer.valueOf(followerFollowing[1]);

    return follower < MAX_FILTER_VALUE && following < MAX_FILTER_VALUE;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
            .getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err
              .println("Usage: ReplicatedJoin <user data> <comment data> <out>");
      System.exit(1);
    }

    String joinType = "inner";

    // Configure the join type
    Job job = Job.getInstance(conf, "Replicated Join, Mapper Job 1");
    job.getConfiguration().set("join.type", joinType);
    job.setJarByClass(TwitterReplicatedJoin.class);

    job.setMapperClass(ReplicatedJoinMapper.class);
    job.setNumReduceTasks(0);

    TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Configure the DistributedCache
   // job.addCacheFile(new Path(otherArgs[0] + "/edges.csv" + "#filelabel").toUri());
    job.addCacheFile(new URI (otherArgs[0] + "/edges.csv" + "#filelabel"));

    int val = job.waitForCompletion(true) ? 0 : 3;
    Counters triangleCounter = job.getCounters();
    Counter totalTriangleCount = triangleCounter.findCounter(TwitterReplicatedJoin.numberOfTriangles.TRIANGLE_COUNT);
    System.out.println("Total number of triangles: " + totalTriangleCount.getValue() / 3);
    System.exit(val);
  }
}
