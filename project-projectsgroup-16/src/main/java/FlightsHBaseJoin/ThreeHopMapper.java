package FlightsHBaseJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import FlightsRSJoin.FlightRecord;
import lombok.var;

public class ThreeHopMapper extends Mapper<Object, Text, Text, Text> {
    Map<String, List<FlightRecord>> map = new HashMap<>();

    @Override
    public void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException {
        Utility.setup(context, map);
    }

    @Override
    public void map(final Object key, Text value, Context context) throws IOException, InterruptedException {
//        context.write(new Text(key.toString() + "---" + value.toString()), new Text());
        var records = value.toString().split("\t");
        var inFlight = (FlightRecord.getFlightRecord(records[1]));
        var outFlight = (FlightRecord.getFlightRecord(records[2]));
        if (inFlight != null && outFlight != null) {
            var listOfFlightsOriginatingAtDestination = map.getOrDefault(outFlight.getDestinationAirportId().toString(), new ArrayList<>());
            for (FlightRecord flightRecord : listOfFlightsOriginatingAtDestination) {
                if (flightRecord.getDestinationAirportId().toString().equals(inFlight.getOriginAirportId().toString())
                    && flightRecord.compareRecordsOnDate(outFlight) > 0
                ) {
                    context.write(new Text(inFlight.getOriginAirportId().toString() +
                                               "->" + inFlight.getDestinationAirportId().toString() +
                                               "->" + records[0] +
                                               "->" + flightRecord.getOriginAirportId().toString() +
                                               "->" + flightRecord.getDestinationAirportId().toString()
                                              )
                        , new Text());
                }
            }
        }
    }
}
