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

public class OneHopMapperHBase extends Mapper<Object, Text, Text, Text> {
    // Map of flight from origin airport to all possible destinations
    Map<String, List<FlightRecord>> map = new HashMap<>();

    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException {
        Utility.setup(context, map);
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        var flightRecord = FlightRecord.getFlightRecord(value.toString());
        if (flightRecord != null) {
            var listOfConnectingFlights = map.getOrDefault(flightRecord.getDestinationAirportId().toString(), new ArrayList<>());
            for (FlightRecord record : listOfConnectingFlights) {
                if (!flightRecord.getOriginAirportId().toString().equals(record.getDestinationAirportId().toString()) && flightRecord.compareRecordsOnDate(record) >= 0) {
                    context.write(new Text(flightRecord.getCsvString()), new Text(record.getCsvString()));
                }
            }
        }
    }


}
