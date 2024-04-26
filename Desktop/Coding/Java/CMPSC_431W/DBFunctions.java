//import java.lang.reflect.Member;
import java.sql.Connection;
//import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
//import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;
import java.sql.Date;
import java.sql.Statement;


public class DBFunctions {
    private static final String DB_URL = "jdbc:postgresql://localhost:5433/Netflix";
    private static final String USER = "postgres";
    private static final String PASS = "1234";

    private Connection connection;

    public DBFunctions() {
        try {
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public boolean isConnected() {
        if (connection != null) {
           return true;
        }
        else{
            return false;
        }
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
    public void insertContent(String type, String title, java.util.Date dateAdded, int releaseYear, String rating, int duration, String castName, String directorName, String locationName) {
        try {
            // Insert into Content table
            String insertContentQuery = "INSERT INTO public.\"Content\" (type, title, date_added, release_year, rating, duration) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement insertContentStatement = connection.prepareStatement(insertContentQuery);
            insertContentStatement.setString(1, type);
            insertContentStatement.setString(2, title);
            insertContentStatement.setDate(3, new java.sql.Date(dateAdded.getTime()));
            insertContentStatement.setInt(4, releaseYear);
            insertContentStatement.setString(5, rating);
            insertContentStatement.setInt(6, duration);
            insertContentStatement.executeUpdate();
    
            // Insert into CastedWith table
            String insertCastedWithQuery = "INSERT INTO public.\"Casted_With\" (casted_in, cast_mem) VALUES (?, ?)";
            PreparedStatement insertCastedWithStatement = connection.prepareStatement(insertCastedWithQuery);
            insertCastedWithStatement.setString(1, title);
            insertCastedWithStatement.setString(2, castName);
            insertCastedWithStatement.executeUpdate();
    
            // Insert into DirectedBy table
            String insertDirectedByQuery = "INSERT INTO public.\"Directed_By\" (directed_work, working_director) VALUES (?, ?)";
            PreparedStatement insertDirectedByStatement = connection.prepareStatement(insertDirectedByQuery);
            insertDirectedByStatement.setString(1, title);
            insertDirectedByStatement.setString(2, directorName);
            insertDirectedByStatement.executeUpdate();
    
            // Insert into FilmedAt table
            String insertFilmedAtQuery = "INSERT INTO public.\"Filmed_At\" (film_title, film_location) VALUES (?, ?)";
            PreparedStatement insertFilmedAtStatement = connection.prepareStatement(insertFilmedAtQuery);
            insertFilmedAtStatement.setString(1, title);
            insertFilmedAtStatement.setString(2, locationName);
            insertFilmedAtStatement.executeUpdate();
    
            System.out.println("Record inserted successfully.");
    
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void deleteData(String title) {
        try {
            // Delete from Content table
            String deleteContentQuery = "DELETE FROM public.\"Content\" WHERE title = ?";
            PreparedStatement deleteContentStatement = connection.prepareStatement(deleteContentQuery);
            deleteContentStatement.setString(1, title);
            deleteContentStatement.executeUpdate();
    
            // Delete related rows from CastedWith, DirectedBy, and FilmedAt tables
            String deleteCastedWithQuery = "DELETE FROM public.\"Casted_With\" WHERE casted_in = ?";
            PreparedStatement deleteCastedWithStatement = connection.prepareStatement(deleteCastedWithQuery);
            deleteCastedWithStatement.setString(1, title);
            deleteCastedWithStatement.executeUpdate();
    
            String deleteDirectedByQuery = "DELETE FROM public.\"Directed_By\" WHERE directed_work = ?";
            PreparedStatement deleteDirectedByStatement = connection.prepareStatement(deleteDirectedByQuery);
            deleteDirectedByStatement.setString(1, title);
            deleteDirectedByStatement.executeUpdate();
    
            String deleteFilmedAtQuery = "DELETE FROM public.\"Filmed_At\" WHERE film_location = ?";
            PreparedStatement deleteFilmedAtStatement = connection.prepareStatement(deleteFilmedAtQuery);
            deleteFilmedAtStatement.setString(1, title);
            deleteFilmedAtStatement.executeUpdate();
    
            System.out.println("Record and related records deleted successfully.");
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    
    public void updateData(String title, String newRating) {
        try {
            String updateQuery = "UPDATE public.\"Content\" SET rating = ? WHERE title = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(updateQuery);
            preparedStatement.setString(1, newRating);
            preparedStatement.setString(2, title);
    
            preparedStatement.executeUpdate();
    
            System.out.println("Record updated successfully.");
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void searchData(String title) {
        try {
            // Search for all cast members in the given work
            String searchQuery = "Select * From public.\"Casted_With\" WHERE casted_in = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(searchQuery);
            preparedStatement.setString(1, title);
            ResultSet resultSet = preparedStatement.executeQuery();
    
            System.out.println("Cast members in " + title + ":");
            while (resultSet.next()) {
                String castMember = resultSet.getString("cast_mem");
                System.out.println("- " + castMember);
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    public void aggregateFunctions() {
        try {
            // Count the number of titles
            String countQuery = "SELECT COUNT(*) AS title_count FROM public.\"Content\"";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(countQuery);
    
            if (resultSet.next()) {
                int titleCount = resultSet.getInt("title_count");
                System.out.println("Number of titles: " + titleCount);
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void sorting(String columnName, String sortOrder) {
        try {
            // Construct the query
            if (columnName.equals("name")){
                columnName="title";
            }
            else{
                columnName="date_added";
            }
            sortOrder.toUpperCase();
            String sortQuery = "SELECT * FROM public.\"Content\" ORDER BY " + columnName + " " + sortOrder;
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sortQuery);
    
            // Print the sorted results
            while (resultSet.next()) {
                // Handle printing of each row based on table structure
                // For example:
                String title = resultSet.getString("title");
                String type = resultSet.getString("type");
                Date dateAdded = resultSet.getDate("date_added");
                int release_year = resultSet.getInt("release_year");
                String rating = resultSet.getString("rating");
                int duration = resultSet.getInt("duration");
                System.out.println(title + ", " + type + ", " + dateAdded + ", " 
                                    + release_year+ ", " + rating+ ", " + duration );
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void showCountries() {
        try {
            // Construct the query
            String sortQuery = "SELECT distinct location_name from public.\"Location\"";
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sortQuery);
    
            while (resultSet.next()) {
                // Handle printing of each row based on table structure
                // For example:
                String country = resultSet.getString("location_name");
                System.out.println(country);
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    public void joins(String Country) {
            try {
                // Construct the query
                String query = "SELECT c.title, c.release_year, l.location_name, cw.cast_mem " +
                "FROM public.\"Content\" c " +
                "JOIN public.\"Filmed_At\" f ON c.title = f.film_title " +
                "JOIN public.\"Location\" l ON f.film_location = l.location_name " +
                "JOIN public.\"Casted_With\" cw ON c.title = cw.casted_in " +
                "WHERE l.location_name = ?";
                PreparedStatement joinQuery = connection.prepareStatement(query);
                joinQuery.setString(1, Country);
                ResultSet resultSet = joinQuery.executeQuery();

                // Print the sorted results
                while (resultSet.next()) {
                    // Handle printing of each row based on table structure
                    // For example:
                    String title = resultSet.getString("title");
                    int release_year = resultSet.getInt("release_year");
                    String location = resultSet.getString("location_name");
                    String member = resultSet.getString("cast_mem");
                    System.out.println(title + ", " + release_year + ", " + location +", " + member);
                }
            } catch (SQLException e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
        public void grouping() {
            try {
                String query = "SELECT c.title, l.location_name " +
                               "FROM public.\"Content\" c " +
                               "JOIN public.\"Filmed_At\" f ON c.title = f.film_title " +
                               "JOIN public.\"Location\" l ON f.film_location = l.location_name " +
                               "GROUP BY l.location_name, c.title " +
                               "ORDER BY l.location_name, c.title";
        
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery();
        
                while (resultSet.next()) {
                    String country = resultSet.getString("location_name");
                    String title = resultSet.getString("title");
            
                    System.out.println("Country: " + country + ", Title: " + title);
                }
            } catch (SQLException e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }

    

        public void subqueries() {
            try {
                String query = "SELECT DISTINCT working_director, COUNT(*) AS num_works " +
                               "FROM public.\"Directed_By\" " +
                               "GROUP BY working_director " +
                               "HAVING COUNT(*) > 1";
        
                PreparedStatement statement = connection.prepareStatement(query);
                ResultSet resultSet = statement.executeQuery();
        
                while (resultSet.next()) {
                    String directorName = resultSet.getString("working_director");
                    int numWorks = resultSet.getInt("num_works");
                    System.out.println("Director: " + directorName +"- " + numWorks);

                }
            } catch (SQLException e) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        public void transaction() {
            try {
                connection.setAutoCommit(false); // Start transaction
        
                // Find the country with the most movies
                String mostMoviesQuery =
                        "SELECT film_location, COUNT(*) AS movie_count " +
                                "FROM public.\"Filmed_At\" " +
                                "WHERE film_location IS NOT NULL " +  // Exclude rows where country is null
                                "GROUP BY film_location " +
                                "ORDER BY COUNT(*) DESC " +
                                "LIMIT 1";
                PreparedStatement mostMoviesStatement = connection.prepareStatement(mostMoviesQuery);
                ResultSet mostMoviesResult = mostMoviesStatement.executeQuery();
        
                if (mostMoviesResult.next()) {
                    String mostMoviesCountry = mostMoviesResult.getString("film_location");
                    int movieCount = mostMoviesResult.getInt("movie_count");
                    System.out.println("Country with the most movies: " + mostMoviesCountry + " (" + movieCount + " movies)");
        
                    // List all movies from the country with the most movies
                    String listMoviesQuery = "SELECT film_title " +
                            "FROM public.\"Filmed_At\" " +
                            "WHERE film_location = ?";
                    PreparedStatement listMoviesStatement = connection.prepareStatement(listMoviesQuery);
                    listMoviesStatement.setString(1, mostMoviesCountry);
                    ResultSet listMoviesResult = listMoviesStatement.executeQuery();
        
                    System.out.println("Movies from " + mostMoviesCountry + ":");
                    while (listMoviesResult.next()) {
                        String movieTitle = listMoviesResult.getString("film_title");
                        System.out.println("- " + movieTitle);
                    }
                } else {
                    System.out.println("No movies found.");
                }
        
                connection.commit(); // Commit transaction
            } catch (SQLException e) {
                try {
                    connection.rollback(); // Rollback transaction if there's an error
                } catch (SQLException rollbackException) {
                    System.err.println("Error rolling back transaction: " + rollbackException.getMessage());
                }
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    connection.setAutoCommit(true); // Reset auto-commit mode
                } catch (SQLException e) {
                    System.err.println("Error resetting auto-commit mode: " + e.getMessage());
                }
            }
        }        
        


    
}
