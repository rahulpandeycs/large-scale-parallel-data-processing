package FlightsRSJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OneHopMapper extends Mapper<Object, Text, Text, FlightRecord> {

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String[] flightData = value.toString().split(",");
        FlightRecord flightRecord;

        try {
            flightRecord = new FlightRecord(
                flightData[0],
                flightData[1],
                flightData[2] + "," + flightData[3],
                flightData[4],
                flightData[5] + "," + flightData[6],
                flightData[7],
                flightData[8],
                flightData[9],
                flightData[10]);
        } catch (IllegalArgumentException e) {
            return;
        }

        flightRecord.setFlag(new Text("in"));
        context.write(flightRecord.getDestinationAirportId(), flightRecord);

        flightRecord.setFlag(new Text("out"));
        context.write(flightRecord.getOriginAirportId(), flightRecord);
    }
}

