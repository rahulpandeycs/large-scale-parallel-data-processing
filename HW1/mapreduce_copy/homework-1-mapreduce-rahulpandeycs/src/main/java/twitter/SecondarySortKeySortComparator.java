package twitter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortKeySortComparator extends WritableComparator {
  protected SecondarySortKeySortComparator(){
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable a, WritableComparable b) {

    Text key1 = (Text) a;
    Text key2 = (Text) b;
    return key1.compareTo(key2);
  }
}
