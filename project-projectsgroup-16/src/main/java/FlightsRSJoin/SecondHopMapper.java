package FlightsRSJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondHopMapper extends Mapper<Object, Text, Text, SecondHopFlightRecord> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] flightData = value.toString().split("\t")[1].split(",");
        SecondHopFlightRecord flightRecord;
        try {
            flightRecord = new SecondHopFlightRecord(
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
        flightRecord.setIntermediateAirportId(new Text(value.toString().split("\t")[0]));
//        context.write(value, null);

        flightRecord.setFlag(new Text("in"));
        context.write(flightRecord.getDestinationAirportId(), flightRecord);

        flightRecord.setFlag(new Text("out"));
        context.write(flightRecord.getOriginAirportId(), flightRecord);
    }
}
