package FlightsHBaseJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Self join
 */
public class HBaseJoin extends Configured implements Tool {
    public static final Logger LOGGER = LogManager.getLogger(HBaseJoin.class);

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new HBaseJoin(), args);
        } catch (final Exception e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Air trip analysis1");
        job.setJarByClass(HBaseJoin.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        job.setMapperClass(OneHopMapperHBase.class);
		job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(args[0] + "/abc.csv").toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/one"));
        int exit = job.waitForCompletion(true) ? 0 : 1;
        if (exit == 1) {
            LOGGER.error("Job terminated earlier");
            System.exit(1);
        }

        final Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "Air trip analysis1 Hop 2");
        job2.setJarByClass(HBaseJoin.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
        job2.setMapperClass(TwoHopMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.addCacheFile(new Path(args[0] + "/abc.csv").toUri());
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/one"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/two"));
        exit = job2.waitForCompletion(true) ? 0 : 1;
        if (exit == 1) {
            LOGGER.error("2 hop Job terminated earlier");
            System.exit(1);
        }

        final Configuration conf3 = getConf();
        final Job job3 = Job.getInstance(conf3, "Air trip analysis1 Hop 3");
        job3.setJarByClass(HBaseJoin.class);
        final Configuration jobConf3 = job3.getConfiguration();
        jobConf3.set("mapreduce.output.textoutputformat.separator", "\t");
        job3.setMapperClass(ThreeHopMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.addCacheFile(new Path(args[0] + "/abc.csv").toUri());
        FileInputFormat.addInputPath(job3, new Path(args[1] + "/two"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/three"));
        exit = job3.waitForCompletion(true) ? 0 : 1;
        if (exit == 1) {
            LOGGER.error("3 hop Job terminated earlier");
            System.exit(1);
        }
        return exit;
    }
}
