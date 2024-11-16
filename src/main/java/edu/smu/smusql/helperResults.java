package edu.smu.smusql;

import java.util.Random;

public class helperResults {

    private static final long MEGABYTE = 1024L * 1024L;
    private static final long nano = 1000000000;

    public static double nanosecondsToSeconds(double nanoSeconds){
        return nanoSeconds * nano;
    }

    public static double getTime(){
        return nanosecondsToSeconds(System.nanoTime());
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    private static String getRandomCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Boston", "Miami", "Seattle", "Austin", "Dallas", "Atlanta", "Denver"};
        return cities[random.nextInt(cities.length)];
    }

    public static TestResult testingCRUDOnlyUserTable( Engine dbEngine, Random random, long numberOfQueries, int numberOfRuns){

        dbEngine.executeSQL("CREATE TABLE users (id, name, age, city)");

        TestResult results = new TestResult();

        for ( int i = 0 ; i < numberOfRuns; i++){
            prepopulateUserTable(random,  dbEngine,  numberOfQueries);
            //System.gc(); 
            updateRandomDataUsersTable(random, dbEngine, numberOfQueries,results);
            //System.gc(); 
            selectFromUsersWithWhere(dbEngine, results);
            //System.gc(); 
            deleteAllData(dbEngine, numberOfQueries, results);
            System.gc(); 
        }

        return results;

    }

    public static double prepopulateUserTable(Random random, Engine dbEngine, long number) {

        double start = System.nanoTime();

        // Insert initial users
        for (long i = 0; i < number; i++) {
            String name = "User" + i;
            int age = 20 + random.nextInt(40); // Ages between 20 and 60
            String city = getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %d, '%s')", i, name, age, city);
            dbEngine.executeSQL(insertCommand);
        
        }

        double end = System.nanoTime();

        double timeTaken = nanosecondsToSeconds(end - start);

        return timeTaken;

    }

    public static TestResult prepopulateUserTable(Random random, Engine dbEngine, long number, TestResult result) {

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = System.nanoTime();


        // Insert initial users
        for (long i = 0; i < number; i++) {
            String name = "User" + i;
            int age = 20 + random.nextInt(40); // Ages between 20 and 60
            String city = getRandomCity(random);
            String insertCommand = String.format("INSERT INTO users VALUES (%d, '%s', %d, '%s')", i, name, age, city);
            dbEngine.executeSQL(insertCommand);
        
        }

        double end = System.nanoTime();
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);
        
        result.updateSelect(timeTaken, actualMemUsed);

        return result;

    }

    private static void selectFromUsers(Engine dbEngine, TestResult result) {

        System.out.println("SELECT ALL");
        String selectQuery = "SELECT * FROM users";

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        dbEngine.executeSQL(selectQuery);

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        result.updateSelect(timeTaken, actualMemUsed);

        

        
    }

    private static void selectFromUsersWithWhere(Engine dbEngine, TestResult result){

        String selectQuery = "SELECT * FROM users WHERE age > 40 AND city = 'New York'";

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        dbEngine.executeSQL(selectQuery);

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        result.updateSelect(timeTaken, actualMemUsed);
    }

    private static void updateRandomDataUsersTable(Random random, Engine dbEngine, long number, TestResult result){
        
        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        for (long i = 0; i < number; i++) {
            int id = random.nextInt((int)number) + 1;
            int newAge = random.nextInt(60) + 20;
            String updateUserQuery = "UPDATE users SET age = " + newAge + " WHERE id = " + id;
            dbEngine.executeSQL(updateUserQuery);
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        result.updateUpdate(timeTaken, actualMemUsed);
    }

    private static void deleteAllData(Engine dbEngine, long number, TestResult result) {

        long beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        double start = nanosecondsToSeconds(System.nanoTime());

        for (long i = 0; i < number; i++) {
            String deleteUserQuery = "DELETE FROM users WHERE id = " + i;
            dbEngine.executeSQL(deleteUserQuery);
        }

        double end = nanosecondsToSeconds(System.nanoTime());
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();

        long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
        double timeTaken = nanosecondsToSeconds(end - start);

        result.updateDelete(timeTaken, actualMemUsed);
    }
}
