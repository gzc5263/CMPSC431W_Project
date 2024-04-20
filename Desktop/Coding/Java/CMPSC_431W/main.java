import java.util.Scanner;

public class main {

    public static void main(String[] args) {
        DBFunctions dbFunctions = new DBFunctions();
        Scanner scanner = new Scanner(System.in);

        try {
            // Your CLI interface code here
            while (true) {
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

                int option = scanner.nextInt();
                scanner.nextLine(); // Consume newline

                switch (option) {
                    case 1:
                        // Insert Data
                        System.out.println("Enter table name:");
                        String tableName = scanner.nextLine();
                        System.out.println("Enter values (comma-separated):");
                        String[] values = scanner.nextLine().split(",");
                        dbFunctions.insertData(tableName, values);
                        break;
                    case 2:
                        // Implement functionality for deleting data
                        break;
                    case 3:
                        // Implement functionality for updating data
                        break;
                    case 4:
                        // Implement functionality for searching data
                        break;
                    case 5:
                        // Implement functionality for aggregate functions
                        break;
                    case 6:
                        // Implement functionality for sorting
                        break;
                    case 7:
                        // Implement functionality for joins
                        break;
                    case 8:
                        // Implement functionality for grouping
                        break;
                    case 9:
                        // Implement functionality for subqueries
                        break;
                    case 10:
                        // Implement functionality for transactions
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
            if (scanner != null) {
                scanner.close();
            }
            dbFunctions.closeConnection(); // Ensure connection is properly closed
        }
    }
}
