package edu.smu.smusql;
import java.util.*;
import edu.smu.smusql.Engine;
import edu.smu.smusql.EngineHashMapv2;
import edu.smu.smusql.EngineHashMapPlusTree;

public class Testing {

    private static final long MEGABYTE = 1024L * 1024L;
    private static final long nano = 1000000000;

    public static double nanosecondsToSeconds(double nanoSeconds){
        return nanoSeconds / nano;
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }
    
    public static void testingCRUDOnlyUserTable( String selected, Engine dbEngine, Random random){

        System.out.println();

        long numberOfQueries = 10;

        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");

        switch(selected){
            case "10":
                numberOfQueries = 10;
                break;

            case "100":
                numberOfQueries = 100;
                break;

            case "10000":
                numberOfQueries = 10000;
                break;

            case "million":
                numberOfQueries = 1000000;
                break;

                
        }
        prepopulateUserTable(random,  dbEngine,  numberOfQueries);
        selectFromUsers(dbEngine);
        updateRandomDataUsersTable(random, dbEngine, numberOfQueries);
        selectFromUsers(dbEngine);
        deleteRandomData(dbEngine, numberOfQueries);
        selectFromUsers(dbEngine);


    }

    public static void testing2tables( Engine dbEngine, Random random){

        dbEngine.executeSQL("CREATE TABLE products (id, name, price, category)");
        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");

        long numberOfQueries = 10;
        prepopulateUserTable(random, dbEngine, numberOfQueries );

        for (int i = 0; i < 10; i++){
            int productId = i;
            String productName = "Product" + productId;
            double price = 50 + (random.nextDouble() * 1000);
            String category = getRandomCategory(random);
            String insertProductQuery = "INSERT INTO products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
            System.out.println(dbEngine.executeSQL(insertProductQuery));
        }

        System.out.println(dbEngine.executeSQL("SELECT * FROM products"));
        selectFromUsers(dbEngine);

    }



    // Helper method to insert random data into users, products, or orders table
    private static void insertRandomData(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Insert into users table
                int id = random.nextInt(10000) + 10000;
                String name = "User" + id;
                int age = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String insertUserQuery = "INSERT INTO users VALUES (" + id + ", '" + name + "', " + age + ", '" + city + "')";
                dbEngine.executeSQL(insertUserQuery);
                break;
            case 1: // Insert into products table
                int productId = random.nextInt(1000) + 10000;
                String productName = "Product" + productId;
                double price = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
                String insertProductQuery = "INSERT INTO products VALUES (" + productId + ", '" + productName + "', " + price + ", '" + category + "')";
                dbEngine.executeSQL(insertProductQuery);
                break;
            case 2: // Insert into orders table
                int orderId = random.nextInt(10000) + 1;
                int userId = random.nextInt(10000) + 1;
                int productIdRef = random.nextInt(1000) + 1;
                int quantity = random.nextInt(10) + 1;
                String insertOrderQuery = "INSERT INTO orders VALUES (" + orderId + ", " + userId + ", " + productIdRef + ", " + quantity + ")";
                dbEngine.executeSQL(insertOrderQuery);
                break;
        }
    }

    // Helper method to randomly select data from tables
    private static void selectRandomData(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(3);
        String selectQuery;
        switch (tableChoice) {
            case 0:
                selectQuery = "SELECT * FROM users";
                break;
            case 1:
                selectQuery = "SELECT * FROM products";
                break;
            case 2:
                selectQuery = "SELECT * FROM orders";
                break;
            default:
                selectQuery = "SELECT * FROM users";
        }
        dbEngine.executeSQL(selectQuery);
    }

    

    // Helper method to update random data in the tables
    private static void updateRandomData(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Update users table
                int id = random.nextInt(10000) + 1;
                int newAge = random.nextInt(60) + 20;
                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
                dbEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Update products table
                int productId = random.nextInt(1000) + 1;
                double newPrice = 50 + (random.nextDouble() * 1000);
                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE id = " + productId;
                dbEngine.executeSQL(updateProductQuery);
                break;
            case 2: // Update orders table
                int orderId = random.nextInt(10000) + 1;
                int newQuantity = random.nextInt(10) + 1;
                String updateOrderQuery = "UPDATE orders SET quantity = " + newQuantity + " WHERE id = " + orderId;
                dbEngine.executeSQL(updateOrderQuery);
                break;
        }
    }

    

    // Helper method to delete random data from tables
    private static void deleteRandomData(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(3);
        switch (tableChoice) {
            case 0: // Delete from users table
                int userId = random.nextInt(10000) + 1;
                String deleteUserQuery = "DELETE FROM users WHERE id = " + userId;
                dbEngine.executeSQL(deleteUserQuery);
                break;
            case 1: // Delete from products table
                int productId = random.nextInt(1000) + 1;
                String deleteProductQuery = "DELETE FROM products WHERE id = " + productId;
                dbEngine.executeSQL(deleteProductQuery);
                break;
            case 2: // Delete from orders table
                int orderId = random.nextInt(10000) + 1;
                String deleteOrderQuery = "DELETE FROM orders WHERE id = " + orderId;
                dbEngine.executeSQL(deleteOrderQuery);
                break;
        }
    }

    // Helper method to execute a complex SELECT query with WHERE, AND, OR, >, <, LIKE
    private static void complexSelectQuery(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(2);  // Complex queries only on users and products for now
        String complexSelectQuery;
        switch (tableChoice) {
            case 0: // Complex SELECT on users
                int minAge = random.nextInt(20) + 20;
                int maxAge = minAge + random.nextInt(30);
                String city = getRandomCity(random);
                complexSelectQuery = "SELECT * FROM users WHERE age > " + minAge + " AND age < " + maxAge;
                break;
            case 1: // Complex SELECT on products
                double minPrice = 50 + (random.nextDouble() * 200);
                double maxPrice = minPrice + random.nextDouble() * 500;
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice + " AND price < " + maxPrice;
                break;
            case 2: // Complex SELECT on products
                double minPrice2 = 50 + (random.nextDouble() * 200);
                String category = getRandomCategory(random);
                complexSelectQuery = "SELECT * FROM products WHERE price > " + minPrice2 + " AND category = " + category;
                break;
            default:
                complexSelectQuery = "SELECT * FROM users";
        }
        dbEngine.executeSQL(complexSelectQuery);
    }

    // Helper method to execute a complex UPDATE query with WHERE
    private static void complexUpdateQuery(Random random,  Engine dbEngine) {
        int tableChoice = random.nextInt(2);  // Complex updates only on users and products for now
        switch (tableChoice) {
            case 0: // Complex UPDATE on users
                int newAge = random.nextInt(60) + 20;
                String city = getRandomCity(random);
                String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE city = '" + city + "'";
                dbEngine.executeSQL(updateUserQuery);
                break;
            case 1: // Complex UPDATE on products
                double newPrice = 50 + (random.nextDouble() * 1000);
                String category = getRandomCategory(random);
                String updateProductQuery = "UPDATE products SET price = " + newPrice + " WHERE category = '" + category + "'";
                dbEngine.executeSQL(updateProductQuery);
                break;
        }
    }

    // Helper method to return a random city
    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    // Helper method to return a random category for products
    private static String getRandomCategory(Random random) {
        String[] categories = {"Electronics", "Appliances", "Clothing", "Furniture", "Toys", "Sports", "Books", "Beauty", "Garden"};
        return categories[random.nextInt(categories.length)];
    }

    // USERS TABLE specific CRUD functionality

    private static void prepopulateUserTable(Random random, Engine dbEngine, long number) {

        System.out.println("Prepopulating users");

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        // Insert initial users
        for (long i = 0; i < number; i++) {
            String name = "User" + i;
            int age = 20 + random.nextInt(40); // Ages between 20 and 60
            String city = getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %d, '%s')", i, name, age, city);
            System.out.println(dbEngine.executeSQL(insertCommand));
        
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        System.out.println("Time elapsed " + timeTaken + " seconds");
        System.out.println("Memory used " + actualMemUsed + " MB");
        System.out.println();

    }

    private static void selectFromUsers(Engine dbEngine) {

        System.out.println("SELECT ALL");
        String selectQuery = "SELECT * FROM users";

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        System.out.println(dbEngine.executeSQL(selectQuery));

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        System.out.println("Time elapsed " + timeTaken + " seconds");
        System.out.println("Memory used " + actualMemUsed + " MB");
        System.out.println();
        
    }

    private static void updateRandomDataUsersTable(Random random, Engine dbEngine, long number){

        System.out.println("UPDATING");
        
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        for (long i = 0; i < number; i++) {
            int id = random.nextInt(10000) + 1;
            int newAge = random.nextInt(60) + 20;
            String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
            System.out.println(dbEngine.executeSQL(updateUserQuery));
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        System.out.println("Time elapsed " + timeTaken + " seconds");
        System.out.println("Memory used " + actualMemUsed + " MB");
        System.out.println();
    }

    private static void deleteRandomData(Engine dbEngine, long number) {

        System.out.println("DELETING");

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        for (long i = 0; i < number; i++) {
            String deleteUserQuery = "DELETE FROM users WHERE id = " + i;
            System.out.println(dbEngine.executeSQL(deleteUserQuery));
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        System.out.println("Time elapsed " + timeTaken + " seconds");
        System.out.println("Memory used " + actualMemUsed + " MB");
        System.out.println();
    }
}
