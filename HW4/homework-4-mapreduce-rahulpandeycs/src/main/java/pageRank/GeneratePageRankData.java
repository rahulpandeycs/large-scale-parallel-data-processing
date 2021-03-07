package pageRank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;

public class GeneratePageRankData {

  public static void main(String[] args) throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter("/home/rahul/Documents/Fall2020/LSPDP/HW4/homework-4-mapreduce-rahulpandeycs/input/pageRank1.txt"));
    double k = 100;
    double startPageRank = (1.0d / (k * k));
    String fileRow = 0 + "," + "" + "," + BigDecimal.valueOf(startPageRank).toPlainString();
    writer.append(fileRow);
    writer.append("\n");

    for (int i = 1; i < k * k; i++) {
      fileRow = i + " " + (i + 1) + "," + BigDecimal.valueOf(startPageRank).toPlainString();
      writer.append(fileRow);
      writer.append("\n");
    }
    writer.close();
  }


}
