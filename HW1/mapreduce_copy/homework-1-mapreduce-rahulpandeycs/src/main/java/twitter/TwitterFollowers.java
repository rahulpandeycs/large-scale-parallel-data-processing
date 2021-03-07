package twitter;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwitterFollowers implements Writable,
        WritableComparable<TwitterFollowers> {
  private String deptNo;
  private String lNameEmpIDPair;

  @Override
  public int compareTo(TwitterFollowers twitterFollowers) {
    return 0;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
