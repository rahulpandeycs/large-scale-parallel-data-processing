package FlightHbaseTrial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

import FlightsHBaseJoin.OneHopMapperHBase;
import FlightsHBaseJoin.TwoHopMapper;

/**
 * Self join
 */
public class HBaseLocalConnectionTest {
    public static final Logger LOGGER = LogManager.getLogger(HBaseLocalConnectionTest.class);

    public static void main(final String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("Student"));

        //Add col fields
        tableDescriptor.addFamily(new HColumnDescriptor("Roll_No"));
        tableDescriptor.addFamily(new HColumnDescriptor("Address"));

        //Execute
        hBaseAdmin.createTable(tableDescriptor);
    }
}
