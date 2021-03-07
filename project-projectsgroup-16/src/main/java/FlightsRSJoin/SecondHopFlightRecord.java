package FlightsRSJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@NoArgsConstructor
public class SecondHopFlightRecord implements WritableComparable<SecondHopFlightRecord> {
    private Text flDate = new Text();
    private Text originAirportId = new Text();
    private Text originCityName = new Text();
    private Text destinationAirportId = new Text();
    private Text destinationCityName = new Text();
    private Text depTime = new Text();
    private Text arrTime = new Text();
    private Text cancelled = new Text();
    private Text distance = new Text();
    @Setter
    private Text flag = new Text();

    @Setter
    private Text intermediateAirportId = new Text();

    public SecondHopFlightRecord(SecondHopFlightRecord flightRecord) {
        this(
            flightRecord.flDate.toString(),
            flightRecord.originAirportId.toString(),
            flightRecord.originCityName.toString(),
            flightRecord.destinationAirportId.toString(),
            flightRecord.destinationCityName.toString(),

            flightRecord.depTime.toString(),
            flightRecord.arrTime.toString(),
            flightRecord.cancelled.toString(),
            flightRecord.distance.toString()
        );
        this.flag = new Text(flightRecord.flag.toString());
        this.intermediateAirportId = new Text(flightRecord.intermediateAirportId.toString());
    }

    public SecondHopFlightRecord(String flDate, String originAirportId, String originCityName, String destinationAirportId,
                                 String destinationCityName, String depTime, String arrTime, String cancelled, String distance) {

        this(new Text(convertEmptyStringsToNull(flDate)),
            new Text(convertEmptyStringsToNull(originAirportId)),
            new Text(convertEmptyStringsToNull(originCityName)),
            new Text(convertEmptyStringsToNull(destinationAirportId)),
            new Text(convertEmptyStringsToNull(destinationCityName)),
            new Text(convertEmptyStringsToNull(depTime)),
            new Text(convertEmptyStringsToNull(arrTime)),
            new Text(convertEmptyStringsToNull(cancelled)),
            new Text(convertEmptyStringsToNull(distance)));
    }

    public SecondHopFlightRecord(Text flDate, Text originAirportId, Text originCityName, Text destinationAirportId,
                                 Text destinationCityName, Text depTime, Text arrTime, Text cancelled, Text distance) {
        this.flDate = flDate;
        this.originAirportId = originAirportId;
        this.originCityName = originCityName;
        this.destinationAirportId = destinationAirportId;
        this.destinationCityName = destinationCityName;
        this.depTime = depTime;
        this.arrTime = arrTime;
        this.cancelled = cancelled;
        this.distance = distance;
    }

    private static String convertEmptyStringsToNull(String s) {
        if (s.equals("")) {
            throw new IllegalArgumentException("String s is null");
        }
        return s;
    }

    public String getCsvString() {
        return
            flDate.toString() + "," +
                originAirportId.toString() + "," +
                originCityName.toString() + "," +
                destinationAirportId.toString() + "," +
                destinationCityName.toString() + "," +
                depTime.toString() + "," +
                arrTime.toString() + "," +
                cancelled.toString() + "," +
                distance.toString() + ",";
    }

    @Override
    public String toString() {
        return "FlightRecord{" +
                   "FL_DATE=" + flDate +
                   ", ORIGIN_AIRPORT_ID=" + originAirportId +
                   ", ORIGIN_CITY_NAME=" + originCityName +
                   ", DEST_AIRPORT_ID=" + destinationAirportId +
                   ", DEST_CITY_NAME=" + destinationCityName +
                   ", DEP_TIME=" + depTime +
                   ", ARR_TIME=" + arrTime +
                   ", CANCELLED=" + cancelled +
                   ", DISTANCE=" + distance +
                   ", INTERMEDIATE_AIRPORT_ID=" + intermediateAirportId +
                   ", flag='" + flag + '\'' +
                   '}';
    }

    public int compareRecordsOnDate(SecondHopFlightRecord flightRecord) {


        LocalDate inputDate = LocalDate.parse(getFormattedDate(flightRecord.flDate.toString()));
        LocalDate currentObjectDate = LocalDate.parse(getFormattedDate(this.flDate.toString()));

        Double localArrTime = Double.parseDouble(arrTime.toString());
        Double localDepTime = Double.parseDouble(flightRecord.depTime.toString());

        if (inputDate.compareTo(currentObjectDate) == 0) {
            return localArrTime.compareTo(localDepTime);
        } else {
            return 1;
        }

    }

    private String getFormattedDate(String s) {
        String[] inputDateStr = s.split("/");
        for (int i = 0; i < 2; i++) {
            if (inputDateStr[i].length() == 1) {
                inputDateStr[i] = "0" + inputDateStr[i];
            }
        }
        if (inputDateStr[2].length() != 4) {
            inputDateStr[2] = String.format("%04d", Integer.parseInt(inputDateStr[2]));
        }
        return inputDateStr[2] + "-" + inputDateStr[0] + "-" + inputDateStr[1];
    }

    @Override
    public int compareTo(SecondHopFlightRecord flightRecord) { //Compare based on dates
        LocalDate inputDate = LocalDate.parse(getFormattedDate(flightRecord.flDate.toString()));
        LocalDate currentObjectDate = LocalDate.parse(getFormattedDate(this.flDate.toString()));

        if (currentObjectDate.compareTo(inputDate) == 0) { //Compare on date
            return this.originAirportId.compareTo(flightRecord.destinationAirportId);
        }
        return currentObjectDate.compareTo(inputDate);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        flDate.write(dataOutput);
        originAirportId.write(dataOutput);
        originCityName.write(dataOutput);
        destinationAirportId.write(dataOutput);
        destinationCityName.write(dataOutput);
        depTime.write(dataOutput);
        arrTime.write(dataOutput);
        cancelled.write(dataOutput);
        distance.write(dataOutput);
        flag.write(dataOutput);
        intermediateAirportId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flDate.readFields(dataInput);
        originAirportId.readFields(dataInput);
        originCityName.readFields(dataInput);
        destinationAirportId.readFields(dataInput);
        destinationCityName.readFields(dataInput);
        depTime.readFields(dataInput);
        arrTime.readFields(dataInput);
        cancelled.readFields(dataInput);
        distance.readFields(dataInput);
        flag.readFields(dataInput);
        intermediateAirportId.readFields(dataInput);
    }
}
