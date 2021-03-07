package twitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupingComparator extends WritableComparator {

  protected SecondarySortGroupingComparator() {
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    Text key1 = (Text) w1;
    Text key2 = (Text) w2;
    return key1.toString().split(",")[1].compareTo(key2.toString().split(",")[1]);
  }
}
