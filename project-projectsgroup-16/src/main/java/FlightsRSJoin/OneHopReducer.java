package FlightsRSJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OneHopReducer extends Reducer<Text, FlightRecord, Text, FlightRecord> {

    @Override
    public void reduce(final Text key, final Iterable<FlightRecord> values, final Context context) throws IOException, InterruptedException {

        List<FlightRecord> inFlights = new ArrayList<>();
        List<FlightRecord> outFlights = new ArrayList<>();

//      if(!key.toString().equals("11298")) return;
//      System.out.println("----------------------------" + key);

        for (FlightRecord val : values) {
//        System.out.println("********************************" + val.toString());
//        context.write(new Text(), val);
            if (val.getFlag().toString().equals("in")) {
                inFlights.add(new FlightRecord(val));
            } else {
                outFlights.add(new FlightRecord(val));
            }
        }

//      for (FlightRecord f : inFlights) {
//                context.write(new Text("in"), f);
//      }
//      for (FlightRecord g : outFlights) {
//        context.write(new Text("out"), g);
//      }

        //      if (inFlights.size() == 0 || outFlights.size() == 0) {
//        System.err.println("one of them is empty");
//        return;
//      }

        for (FlightRecord outFlight : outFlights) {
            for (FlightRecord inFlight : inFlights) {
                if (inFlight.getOriginAirportId().toString().equals(outFlight.getDestinationAirportId().toString())) {
                    continue;
                }
                if (inFlight.compareRecordsOnDate(outFlight) >= 0) {
//
//            System.out.println("********************************" + inFlight.toString());
//            System.out.println("********************************" + outFlight.toString());

//            System.out.println( "________________________________" +
//                    inFlight.getFlDate() + " " +
//                    inFlight.getOriginAirportId() + " " +
//                    inFlight.getOriginCityName() + " " +
//                    inFlight.getDestinationAirportId() + " " +
//                    inFlight.getDestinationCityName() + " " +
//
//                    outFlight.getOriginAirportId()+ " " +
//                    outFlight.getOriginCityName() + " " +
//                    outFlight.getDestinationAirportId()+ " " +
//                    outFlight.getDestinationCityName() + " " +
//
//                    inFlight.getDepTime() + " " +
//                    inFlight.getArrTime() + " " +
//                    outFlight.getDepTime() + " " +
//                    outFlight.getArrTime() + " " +
//                    inFlight.getDistance() + " " +
//                    outFlight.getDistance());

                    FlightRecord outValue = new FlightRecord(
                        inFlight.getFlDate(),
                        inFlight.getOriginAirportId(),
                        inFlight.getOriginCityName(),
                        outFlight.getDestinationAirportId(),
                        outFlight.getDestinationCityName(),
                        inFlight.getDepTime(),
                        outFlight.getArrTime(),
                        inFlight.getCancelled(),
                        new Text(String.valueOf(Double.parseDouble(inFlight.getDistance().toString()) + Double.parseDouble(outFlight.getDistance().toString())))
                    );
//            System.out.println("____________________" + outValue);
                    context.write(new Text(key.toString() + "\t" + outValue.getCsvString()), outValue);
                }
            }
        }

//      System.out.println("____________________" + inFlights.size() + " " + outFlights.size());
    }
}
