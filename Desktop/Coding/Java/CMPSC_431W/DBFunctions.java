import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

class DBFunctions {

    // Database connection details
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/Netflix";
    private static final String USER = "postgres";
    private static final String PASS = "1234";

    private Connection connection;

    public DBFunctions() {
        try {
            this.connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String tableName, String[] values) {
        try (Statement stmt = connection.createStatement()) {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("INSERT INTO ").append(tableName).append(" VALUES (");
            for (String value : values) {
                queryBuilder.append("'").append(value).append("', ");
            }
            queryBuilder.setLength(queryBuilder.length() - 2); // Remove the last comma and space
            queryBuilder.append(")");

            // Execute the query
            stmt.executeUpdate(queryBuilder.toString());
            System.out.println("Data inserted successfully");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteData(String tableName, String condition) {
        // Implement functionality for deleting data
        System.out.println("Not Yet Implemented for Delete Data");
    }

    public void updateData(String tableName, String column, String newValue, String condition) {
        // Implement functionality for updating data
        System.out.println("Not Yet Implemented for Update Data");
    }

    public void searchData(String tableName, String condition) {
        // Implement functionality for searching data
        System.out.println("Not Yet Implemented for Search Data");
    }

    public void aggregateFunctions(String tableName, String column) {
        // Implement functionality for aggregate functions
        System.out.println("Not Yet Implemented for Aggregate Functions");
    }

    public void sorting(String tableName, String column, String order) {
        // Implement functionality for sorting
        System.out.println("Not Yet Implemented for Sorting");
    }

    public void joins(String table1, String table2, String key) {
        // Implement functionality for joins
        System.out.println("Not Yet Implemented for Joins");
    }

    public void grouping(String tableName, String column) {
        // Implement functionality for grouping
        System.out.println("Not Yet Implemented for Grouping");
    }

    public void subqueries(String tableName, String subquery) {
        // Implement functionality for subqueries
        System.out.println("Not Yet Implemented for Subqueries");
    }

    public void transactions() {
        // Implement functionality for transactions
        System.out.println("Not Yet Implemented for Transactions");
    }

    public void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
