package FlightsHBaseJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import FlightsRSJoin.FlightRecord;
import lombok.var;

public class Utility {
    protected static void setup(Mapper<Object, Text, Text, Text>.Context context, Map<String, List<FlightRecord>> map) throws IOException {
        int x = 0;
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            var reader = new BufferedReader(new FileReader("abc.csv"));
            var line = reader.readLine();
            while (line != null) {
                if (++x % 100 == 0) {
                    var flightRecord = FlightRecord.getFlightRecord(line);
                    if (flightRecord != null) {
                        var list = map.getOrDefault(flightRecord.getOriginAirportId().toString(), new ArrayList<>());
                        list.add(flightRecord);
                        map.put(flightRecord.getOriginAirportId().toString(), list);
                    }
                }
                line = reader.readLine();
            }
        }
    }
}
