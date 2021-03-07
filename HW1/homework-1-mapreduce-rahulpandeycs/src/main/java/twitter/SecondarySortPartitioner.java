package twitter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<Text, IntWritable> {
  @Override
  public int getPartition(Text text, IntWritable intWritable, int numReduceTasks) {
    return (text.toString().split(",")[0].hashCode() % numReduceTasks);
  }
}
