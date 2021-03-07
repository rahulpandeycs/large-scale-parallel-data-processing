package FlightsRSJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RSJoinMain extends Configured implements Tool {
    public static final Logger LOGGER = LogManager.getLogger(RSJoinMain.class);

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RSJoinMain(), args);
        } catch (final Exception e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Air trip analysis");
        job.setJarByClass(RSJoinMain.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        job.setMapperClass(OneHopMapper.class);
//		job.setCombinerClass(OneHopReducer.class);
//		job.setNumReduceTasks(0);
        job.setReducerClass(OneHopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/one"));

        int exit = job.waitForCompletion(true) ? 0 : 1;
        if (exit == 1) {
            LOGGER.error("Job terminated earlier");
            System.exit(1);
        }
        final Configuration conf2 = getConf();
        final Job job2 = Job.getInstance(conf2, "2 Hop");
        job2.setJarByClass(RSJoinMain.class);
        final Configuration jobConf2 = job.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
//        FileInputFormat.addInputPath(job2, new Path(args[1] + "/one"));
        MultipleInputs.addInputPath(job2, new Path(args[1] + "/one"), TextInputFormat.class, SecondHopMapper.class);
//        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class,OneHopMapper.class);
//        job2.setMapperClass(SecondHopMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(SecondHopFlightRecord.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/two"));
        job2.setReducerClass(SecondHopReducer.class);
//        job2.setNumReduceTasks(0);
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}
