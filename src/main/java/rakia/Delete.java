package rakia;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import rakia.distillery.RakiaDistillery;
import rakia.workers.Worker;
import rakia.workers.WorkerPOJO;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Delete {
    public static void main(String[] args) {
        RakiaDistillery rakiaDistillery = new RakiaDistillery();


        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/rakia_db", "root", "rootpass");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        ResultSet resultSet = null;
        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(
            "SELECT full_name, age FROM gatherers " +
                "UNION ALL " +
                "SELECT full_name, age FROM distillers;"
            );
        } catch (SQLException e) {
            e.printStackTrace();
        }

        List<WorkerPOJO> allWorkers = new ArrayList<>();
        try {
            while (resultSet.next()) {
                allWorkers.add(new WorkerPOJO(resultSet.getString("full_name"), resultSet.getInt("age")));
            }

            ObjectMapper objectMapper = new ObjectMapper();
            XmlMapper xmlMapper = new XmlMapper();


            objectMapper.writeValue(new File("deleteFile.json"), allWorkers);
            xmlMapper.writeValue(new File("deleteFile.xml"), allWorkers);

//
//            StringBuffer jsonBuffer = new StringBuffer();
//            StringBuffer xmlBuffer = new StringBuffer();
//            jsonBuffer.append("[");
//            for (WorkerPOJO w : allWorkers) {
//                jsonBuffer.append(objectMapper.writeValueAsString(w)).append(", ");
//                xmlBuffer.append(xmlMapper.writeValueAsString(w));
//            }
//            jsonBuffer.append("]");
//            Files.write(Path.of("C:\\ITT Code\\Test-3\\Rakia\\src\\main\\java\\rakia\\workers.json"), jsonBuffer.toString().getBytes(), StandardOpenOption.CREATE);
//            Files.write(Path.of("C:\\ITT Code\\Test-3\\Rakia\\src\\main\\java\\rakia\\workers.xml"), xmlBuffer.toString().getBytes(), StandardOpenOption.CREATE);

        } catch (SQLException e ) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
