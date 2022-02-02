package rakia.distillery;

import rakia.workers.Distiller;
import rakia.workers.Gatherer;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Locale;

public class LoggerMySQL extends Thread {
    private RakiaDistillery myDistillery;
    private Connection connection = null;

    //TODO: fields for URL to MySQL rakia_db

    public LoggerMySQL(RakiaDistillery myDist) {
        this.setDaemon(true);
        this.myDistillery = myDist;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/rakia_db", "root", "rootpass");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        insertGatherers();
        insertDistillers();
        createTableRakias();
    }

    private void insertGatherers() {
        try {
            PreparedStatement pst = connection.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS gatherers(" +
                        "id int AUTO_INCREMENT PRIMARY KEY," +
                        "full_name varchar(50) NOT NULL, " +
                        "age int NOT NULL);");
            pst.executeUpdate();

            final int batchSize = myDistillery.getAllGatherers().size();
            connection.setAutoCommit(false);
            pst = connection.prepareStatement("INSERT INTO gatherers (full_name, age) VALUES (?, ?);");
            for (Gatherer g : myDistillery.getAllGatherers()) {
                pst.setString(1, g.getFullName());
                pst.setInt(2, g.getAge());
                pst.addBatch();
            }
            int[] rowsAffected = pst.executeBatch();
            connection.commit();

            System.out.println("INSERTED GATHERERS, ROWS AFFECTED: " + Arrays.toString(rowsAffected));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void insertDistillers() {
        try {
            PreparedStatement pst = connection.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS distillers(" +
                        "id varchar(100) PRIMARY KEY," +
                        "full_name varchar(50) NOT NULL, " +
                        "age int NOT NULL);");
            pst.executeUpdate();

            final int batchSize = myDistillery.getAllGatherers().size();
            connection.setAutoCommit(false);
            pst = connection.prepareStatement("INSERT INTO distillers (id, full_name, age) VALUES (?, ?, ?);");
            for (Distiller d : myDistillery.getAllDistillers()) {
                pst.setString(1, String.valueOf(d.getId()));
                pst.setString(2, d.getFullName());
                pst.setInt(3, d.getAge());
                pst.addBatch();
            }
            int[] rowsAffected = pst.executeBatch();
            connection.commit();

            System.out.println("INSERTED DISTILLERS, ROWS AFFECTED: " + Arrays.toString(rowsAffected));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void createTableRakias() {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS rakias(" +
                                        "id int AUTO_INCREMENT PRIMARY KEY," +
                                        "fruit varchar(50) NOT NULL, " +
                                        "litres int NOT NULL," +
                                        "time TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)," +     //precision 1 millisecond
                                        "distiller_id varchar(100), " +
                                        "FOREIGN KEY (distiller_id) REFERENCES distillers(id) ON UPDATE CASCADE ON DELETE SET NULL);");
            System.out.println("CREATED TABLE rakias");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean insertRakia(Fruit currentFruit, int litresDistilled, String distillerId) {
        try {
            PreparedStatement pst = connection.prepareStatement(
                    "INSERT INTO rakias (fruit, litres, distiller_id) VALUES (?, ?, ?)");
            pst.setString(1, currentFruit.toString());
            pst.setInt(2, litresDistilled);
            pst.setString(3, distillerId);

            if (pst.executeUpdate() == 1) {
                System.out.println("***     INSERTED RAKIA. ROWS AFFECTED: 1\n");
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }



    ResultSet conductSelectQuery(String sql) {
        if (sql == null || sql.isEmpty() || !sql.toUpperCase().startsWith("SELECT")) {
            System.out.println("Invalid query.");
            return null;
        }
        try {
            Statement st = connection.createStatement();
            return st.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void printStats() {
        System.out.println("\n*************** STATS: ******************");
        try {
            Statement st = connection.createStatement();
            ResultSet resultSet = st.executeQuery(
                    "SELECT fruit, SUM(litres) AS total_litres " +
                        "FROM rakias " +
                        "GROUP BY fruit;"
            );
            System.out.println("TOTAL OF: ");
            while (resultSet.next()) {
                System.out.println(resultSet.getInt("total_litres") + " litres " +
                                    resultSet.getString("fruit").toLowerCase() + " rakia");
            }

            resultSet = st.executeQuery(
                    "SELECT d.full_name, SUM(r.litres) AS total " +
                        "FROM rakias AS r " +
                        "JOIN distillers AS d ON (d.id = r.distiller_id) " +
                        "GROUP BY r.distiller_id " +
                        "ORDER by total DESC " +
                        "LIMIT 1;"
            );

            while (resultSet.next()) {
                System.out.println("Distiller with most litres: " + resultSet.getString("full_name") +
                        " - " + resultSet.getString("total") + " litres.");
            }

            resultSet = st.executeQuery(
                    "SELECT full_name, MIN(age) AS age " +
                        "FROM gatherers;"
            );

            while (resultSet.next()) {
                System.out.println("Youngest gatherer: " + resultSet.getString("full_name") +
                        " - " + resultSet.getString("age") + " y.o.");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
