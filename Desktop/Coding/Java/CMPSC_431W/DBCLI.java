import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;

public class DBCLI {

    private static final Scanner scanner = new Scanner(System.in);
    private static final DBFunctions dbFunctions = new DBFunctions();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        try {
            while (dbFunctions.isConnected()) {
                System.out.println("");
                displayMenu();
                int option = scanner.nextInt();
                scanner.nextLine(); // Consume newline

                switch (option) {
                    case 1:
                        insertData();
                        break;
                    case 2:
                        deleteData();
                        break;
                    case 3:
                        updateData();
                        break;
                    case 4:
                        searchData();
                        break;
                    case 5:
                        aggregateFunctions();
                        break;
                    case 6:
                        sorting();
                        break;
                    case 7:
                        joins();
                        break;
                    case 8:
                        grouping();
                        break;
                    case 9:
                        subqueries();
                        break;
                    case 10:
                        transactions();
                        break;
                    case 11:
                        System.out.println("Exiting...");
                        return;
                    default:
                        System.out.println("Invalid option");
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanner.close();
            dbFunctions.closeConnection();
        }
    }

    private static void displayMenu() {
        System.out.println("Database CLI");
        System.out.println("1. Insert Data");
        System.out.println("2. Delete Data");
        System.out.println("3. Update Data");
        System.out.println("4. Search Data");
        System.out.println("5. Aggregate Functions");
        System.out.println("6. Sorting");
        System.out.println("7. Joins");
        System.out.println("8. Grouping");
        System.out.println("9. Subqueries");
        System.out.println("10. Transactions");
        System.out.println("11. Exit");
        System.out.println("Enter option:");
    }

    private static void insertData() {
        try {
            System.out.println("Enter type:");
            String type = scanner.nextLine();
            System.out.println("Enter title:");
            String title = scanner.nextLine();
            System.out.println("Enter date added (YYYY-MM-DD):");
            String dateAddedStr = scanner.nextLine();
            java.util.Date parsedDate = dateFormat.parse(dateAddedStr);
            Date dateAdded = new Date(parsedDate.getTime());

            System.out.println("Enter release year:");
            int releaseYear = scanner.nextInt();
            scanner.nextLine();
            System.out.println("Enter rating:");
            String rating = scanner.nextLine();
            System.out.println("Enter duration:");
            int duration = scanner.nextInt();
            scanner.nextLine();
            System.out.println("Enter cast name:");
            String castName = scanner.nextLine();
            System.out.println("Enter director name:");
            String directorName = scanner.nextLine();
            System.out.println("Enter location name:");
            String locationName = scanner.nextLine();

            dbFunctions.insertContent(type, title, dateAdded, releaseYear, rating, duration, castName, directorName, locationName);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void deleteData() {
        System.out.println("Enter the title to delete:");
        String title = scanner.nextLine();
        dbFunctions.deleteData(title);
    }

    private static void updateData() {
        System.out.println("Enter title name to change rating");
        String Title = scanner.nextLine();
        System.out.println("New Rating:");
        String newRating = scanner.nextLine();
        dbFunctions.updateData(Title, newRating);
    }

    private static void searchData() {
        System.out.println("Finding Cast Members:");
        System.out.println("Enter Title in Question:");
        String searchTitle = scanner.nextLine();
        dbFunctions.searchData(searchTitle);
    }

    private static void aggregateFunctions() {
        System.out.println("Counting how many entries exist");
        dbFunctions.aggregateFunctions();
    }

    private static void sorting() {
        try {
            System.out.println("Sorting Content");
            System.out.println("Enter column name (name or date):");
            String sortColumn = scanner.nextLine();
            System.out.println("Enter order (ASC/DESC):");
            String sortOrder = scanner.nextLine();

            dbFunctions.sorting(sortColumn, sortOrder);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void joins() {
        System.out.println("Here are a list of countries");
        dbFunctions.showCountries();
        System.out.println("Please select a country");
        String CountrySelected = scanner.nextLine();
        dbFunctions.joins(CountrySelected);
    }

    private static void grouping() {
        System.out.println("Sorting By country");
        dbFunctions.grouping();
    }

    private static void subqueries() {
        System.out.println("Showing Multiple Distinct Works by Director");
        dbFunctions.subqueries();
    }

    private static void transactions() {
        System.out.println("Showing Which countries have the most works then showing ");

        dbFunctions.transaction();
    }
}
