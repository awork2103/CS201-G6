package edu.smu.smusql;
import java.util.*;
import edu.smu.smusql.Engine;
import edu.smu.smusql.EngineSeparateChainingHashMap;
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
    
    public static void testingCRUDOnlyUserTable( Engine dbEngine, Random random, long numberOfQueries){

        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");
        
        prepopulateUserTable(random,  dbEngine,  numberOfQueries);
        System.gc(); 
        selectFromUsers(dbEngine);
        System.gc();
        updateRandomDataUsersTable(random, dbEngine, numberOfQueries);
        System.gc(); 
        selectFromUsers(dbEngine);
        selectFromUsersWithWhere(dbEngine);
        System.gc(); 
        deleteAllData(dbEngine, numberOfQueries);
        selectFromUsers(dbEngine);
        System.gc(); 

    }

    public static void testing2tables( Engine dbEngine, Random random){

        dbEngine.executeSQL("CREATE TABLE products (id, name, price, category)");
        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");

        long numberOfQueries = 1000;
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

        // System.out.println("Time elapsed " + timeTaken + " seconds");
        // System.out.println("Memory used " + actualMemUsed + " MB");
        // System.out.println();

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

        // System.out.println("Time elapsed " + timeTaken + " seconds");
        // System.out.println("Memory used " + actualMemUsed + " MB");
        // System.out.println();
        
    }

    private static void selectFromUsersWithWhere(Engine dbEngine){
        System.out.println("SELECT WITH WHERE age > 40 OR city = 'New York'");

        String selectQuery = "SELECT * FROM users WHERE age > 40 OR city = 'New York'"; // 

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        System.out.println(dbEngine.executeSQL(selectQuery));

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        // System.out.println("Time elapsed " + timeTaken + " seconds");
        // System.out.println("Memory used " + actualMemUsed + " MB");
        // System.out.println();
    }

    private static void updateRandomDataUsersTable(Random random, Engine dbEngine, long number){

        System.out.println("UPDATING");
        System.out.println("VALUUE WILL BE UPDATED TO 42");
        
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        for (long i = 0; i < number; i++) {
            int id = random.nextInt((int)number) + 1;
            int newAge = 42;
            String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
            System.out.println(dbEngine.executeSQL(updateUserQuery));
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        // System.out.println("Time elapsed " + timeTaken + " seconds");
        // System.out.println("Memory used " + actualMemUsed + " MB");
        // System.out.println();
    }

    private static void deleteAllData(Engine dbEngine, long number) {

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

        // System.out.println("Time elapsed " + timeTaken + " seconds");
        // System.out.println("Memory used " + actualMemUsed + " MB");
        // System.out.println();
    }
}
