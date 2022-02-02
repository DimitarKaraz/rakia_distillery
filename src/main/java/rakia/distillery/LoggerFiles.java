package rakia.distillery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import rakia.workers.Worker;
import rakia.workers.WorkerPOJO;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class LoggerFiles extends Thread {
    private RakiaDistillery myDistillery;

    public LoggerFiles(RakiaDistillery myDistillery) {
        this.setDaemon(true);
        this.myDistillery = myDistillery;
    }

    @Override
    public void run() {
        int counter = 1;
        StringBuffer stringBuffer = new StringBuffer();
        while (true) {
            stringBuffer.setLength(0);
            try {
                Thread.sleep(3000);

                ResultSet resultSet = myDistillery.sqlLogger.conductSelectQuery(
                        "SELECT fruit, SUM(litres) AS total_litres\n" +
                            "FROM rakias\n" +
                            "GROUP BY fruit\n" +
                            "ORDER BY total_litres DESC\n" +
                            "LIMIT 1;"
                );
                while (resultSet.next()) {
                    stringBuffer.append("Most distilled type of rakia: ")
                                .append(resultSet.getString("fruit"))
                                .append(", ")
                                .append(resultSet.getInt("total_litres"))
                                .append(" litres so far.\n");
                }

                resultSet = myDistillery.sqlLogger.conductSelectQuery(
                        "SELECT SUM(litres) / (\n" +
                                "\tSELECT SUM(litres)\n" +
                                "    FROM rakias\n" +
                                "    WHERE fruit = \"APRICOT\"\n" +
                                ") AS ratio\n" +
                                "FROM rakias\n" +
                                "WHERE fruit = \"GRAPE\";"
                );

                while (resultSet.next()) {
                    stringBuffer.append("Ratio Grape (L) / Apricot (L) so far: ")
                            .append(resultSet.getString("ratio"));
                }

                Files.write(Path.of("C:\\ITT Code\\Test-3\\Rakia\\src\\main\\java\\rakia\\Stats" + counter++ + ".txt"),
                        stringBuffer.toString().getBytes(), StandardOpenOption.CREATE);

                System.out.println("FILE CREATED            ***");
            } catch (InterruptedException | IOException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void exportFiles() {
        ResultSet resultSet = myDistillery.sqlLogger.conductSelectQuery(
                "SELECT full_name, age FROM gatherers " +
                    "UNION ALL " +
                    "SELECT full_name, age FROM distillers;"
        );

        List<WorkerPOJO> allWorkers = new ArrayList<>();
        try {
            while (resultSet.next()) {
                allWorkers.add(new WorkerPOJO(resultSet.getString("full_name"), resultSet.getInt("age")));
            }

            ObjectMapper objectMapper = new ObjectMapper();
            XmlMapper xmlMapper = new XmlMapper();

            objectMapper.writeValue(new File("C:\\ITT Code\\Test-3\\Rakia\\src\\main\\java\\rakia\\workers.json"), allWorkers);
            xmlMapper.writeValue(new File("C:\\ITT Code\\Test-3\\Rakia\\src\\main\\java\\rakia\\workers.xml"), allWorkers);

        } catch (SQLException e ) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
