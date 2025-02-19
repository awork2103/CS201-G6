package edu.smu.smusql;

import edu.smu.smusql.Testing;
import edu.smu.smusql.helperResults;
import java.util.*;

public class Results {

    int numberOfRuns = 5;
    int hashMultiplier = 31;

    private static final long MEGABYTE = 1024L * 1024L;
    private static final long nano = 1000000000;

    // public static double nanosecondsToSeconds(double nanoSeconds){
    // return nanoSeconds / nano;
    // }

    // public static long bytesToMegabytes(long bytes) {
    // return bytes / MEGABYTE;
    // }

    public static long getMem() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    public static double getTime() {
        return helperResults.nanosecondsToSeconds(System.nanoTime());
    }

    public void testHashFunction() {

        String[] toTest = { "BITWISE", "ADDITIVE", "POLYNOMIAL", "CYCLIC" };

        String hashValue = "hashtest";

        for (String hashCode : toTest) {

            System.out.println(hashCode);

            long beforeUsedMem = getMem();
            double start = getTime();

            for (int i = 0; i < 10000; i++) {
                customHashCode(hashValue, hashCode);

            }

            double end = getTime();
            long afterUsedMem = getMem();
            long actualMemUsed = helperResults.bytesToMegabytes(afterUsedMem - beforeUsedMem);
            double timeTaken = helperResults.nanosecondsToSeconds(end - start);

            // System.out.println("Time elapsed :" + timeTaken + " seconds");
            System.out.println("Individual time :" + timeTaken / 10000 + " seconds");
            System.out.println("Memory used " + actualMemUsed + " MB");
            System.out.println();

            System.gc();

        }

    }

    public void testHashMapWithTreesInsertionComparison() {
        EngineHashMapPlusTree treeEngine = new EngineHashMapPlusTree();
        Engine normalEngine = new Engine();
        long numberOfInsertions = 10000;

        treeEngine.executeSQL("CREATE TABLE users (id, name, age, city)");
        normalEngine.executeSQL("CREATE TABLE users (id, name, age, city)");

        double treeEngineInsertionTime = 0;
        long treeEngineMem = 0;
        double engineInsertionTime = 0;
        long engineMem = 0;

        long numberOfTests = 5;

        // Testing methodology is to take the average time and memory of 5 tests

        for (int i = 0; i < numberOfTests; i++) {
            treeEngineInsertionTime += helperResults.prepopulateUserTable(new Random(), treeEngine, numberOfInsertions);
            treeEngineMem += treeEngine.getMemConsumptionForTable("users");
            System.gc();
        }

        System.out.println("TreeEngine results");
        System.out.println("Memory consumption : " + helperResults.bytesToMegabytes(treeEngineMem / 5) + " MB");
        System.out.println("Average time taken : " + treeEngineInsertionTime / 5 + " s");

        for (int i = 0; i < numberOfTests; i++) {
            engineInsertionTime += helperResults.prepopulateUserTable(new Random(), normalEngine, numberOfInsertions);
            engineMem += normalEngine.getMemConsumptionForTable("users");
            System.gc();
        }

        System.out.println("Engine results");
        System.out.println("Memory consumption : " + helperResults.bytesToMegabytes(engineMem / 5) + " MB");
        System.out.println("Average time taken : " + engineInsertionTime / 5 + " s");

    }

    public void crudTestingTreeMapAndNormal() {

        long numberOfQueries = 5000;
        TestResult HashtreeMapResults;
        TestResult controlEngineResults;
        int numOfRuns = 5;

        System.out.println("Testing HashMap with Trees");

        HashtreeMapResults = helperResults.testingCRUDOnlyUserTable(new EngineHashMapPlusTree(), new Random(),
                numberOfQueries, numOfRuns);
        HashtreeMapResults.results(numOfRuns);

        System.out.println();
        System.out.println();
        System.out.println("Testing control engine");
        controlEngineResults = helperResults.testingCRUDOnlyUserTable(new Engine(), new Random(), numberOfQueries,
                numOfRuns);
        controlEngineResults.results(numOfRuns);
    }

    public void crudTestingLoadFactorsChaining() {
        long numberOfQueries = 6000;

        TestResult chaining01Result;
        TestResult chaining05Result;
        TestResult chaining075Result;
        TestResult chaining09Result;
        TestResult chaining2Result;

        System.out.println("Seperate chaining Loadfactors tests");
        System.out.println("Number of runs : " + numberOfRuns);
        System.out.println("Number of queries : " + numberOfQueries);
        // System.out.println("LOADFACTOR : 0.1");
        // chaining01Result = helperResults.testingCRUDOnlyUserTable(new
        // EngineSeparateChainingHashMap(0.1), new Random(), numberOfQueries,
        // hashMultiplier);
        // chaining01Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.5");
        chaining05Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.5), new Random(),
                numberOfQueries, numberOfRuns);
        chaining05Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.75");
        chaining075Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.75),
                new Random(), numberOfQueries, numberOfRuns);
        chaining075Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.9");
        chaining09Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.9), new Random(),
                numberOfQueries, numberOfRuns);
        chaining09Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 2");
        chaining2Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(2), new Random(),
                numberOfQueries, numberOfRuns);
        chaining2Result.results(numberOfQueries);

    }

    public void crudTestingLoadFactorsLinear() {

        long numberOfQueries = 5000;

        TestResult linearProbe01Result;
        TestResult linearProbe05Result;
        TestResult linearProbe075Result;
        TestResult linearProbe09Result;

        System.out.println("Linear probing Loadfactors tests");
        System.out.println("Number of runs : " + numberOfRuns);
        System.out.println("Number of queries : " + numberOfQueries);
        // System.out.println("LOADFACTOR : 0.1");
        // linearProbe01Result = helperResults.testingCRUDOnlyUserTable(new
        // EngineLinearProbeHashMap(0.1), new Random(), numberOfQueries,
        // hashMultiplier);
        // linearProbe01Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.5");
        linearProbe05Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.5),
                new Random(), numberOfQueries, numberOfRuns);
        linearProbe05Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.75");
        linearProbe075Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.75),
                new Random(), numberOfQueries, numberOfRuns);
        linearProbe075Result.results(numberOfQueries);

        System.out.println("LOADFACTOR : 0.9");
        linearProbe09Result = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap(0.9),
                new Random(), numberOfQueries, numberOfRuns);
        linearProbe09Result.results(numberOfQueries);
    }

    public void crudTestingHashingFunctions() {

        long numberOfQueries = 1000;

        TestResult defaultResult;
        TestResult bitwiseResult;
        TestResult additiveResult;
        TestResult polynomialResult;
        TestResult cyclicResult;

        System.out.println("DEFAULT");

        defaultResult = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap("DEFAULT"),
                new Random(), numberOfQueries, numberOfRuns);
        defaultResult.results(numberOfRuns);

        System.out.println("BITWISE");

        bitwiseResult = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap("BITWISE"),
                new Random(), numberOfQueries, numberOfRuns);
        bitwiseResult.results(numberOfRuns);

        System.out.println("ADDITIVE");

        additiveResult = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap("ADDITIVE"),
                new Random(), numberOfQueries, numberOfRuns);
        additiveResult.results(numberOfRuns);

        System.out.println("POLYNOMIAL");

        polynomialResult = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap("POLYNOMIAL"),
                new Random(), numberOfQueries, numberOfRuns);
        polynomialResult.results(numberOfRuns);

        System.out.println("CYCLIC");

        cyclicResult = helperResults.testingCRUDOnlyUserTable(new EngineSeparateChainingHashMap("CYCLIC"), new Random(),
                numberOfQueries, numberOfRuns);
        cyclicResult.results(numberOfRuns);

    }

    public static void main(String[] args) {

        Results testingobj = new Results();

        // Tests the memory and time consumption of HashMap vs HashMap with Trees after
        // populating the table
        testingobj.testHashMapWithTreesInsertionComparison();

        // Testing CRUD between the HashMap vs HashMap with Trees
        // testingobj.crudTestingTreeMapAndNormal();

        // Tests the hashing time for each hashing strategy
        // ADDITIVE, BITWISE, POLYNOMIAL, CYCLIC
        // testingobj.testHashFunction();

        // Testing CRUD when using different hashing functions
        // Tested with CustomHashMapChaining
        // testingobj.crudTestingHashingFunctions();

        // Tests CRUD for varying load factors for both separate chaining and linear probing
        // The below 2 should be run togther
        testingobj.crudTestingLoadFactorsChaining();
        testingobj.crudTestingLoadFactorsLinear();

    }

    // Our Hashing strategies
    private int customBitwiseHashCode(String key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((int) str.charAt(i));
        }

        return hash;
    }

    // Polynomial Hashing
    private int customPolynomialHashCode(String key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash *= hashMultiplier;
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Cyclic Hashing
    private int customCyclicHashCode(String key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << 5) | (hash >>> 27);
            hash += (int) str.charAt(i);
        }

        return hash;
    }

    // Additive Hahsing --> Similar to polynomial but without pow(multiplier) by i
    private int customAdditiveHashCode(String key) {
        // Typecast Key into a String (since ID is a string)
        String str = key.toString();
        int hash = 0;

        // Custom Hash Functiom with set multiplier
        for (int i = 0; i < str.length(); i++) {
            hash += hashMultiplier * (int) str.charAt(i);
        }

        return hash;
    }

    private int customHashCode(String key, String hashingStrategy) {
        switch (hashingStrategy) {
            case "BITWISE":
                return customBitwiseHashCode(key);
            case "POLYNOMIAL":
                return customPolynomialHashCode(key);
            case "CYCLIC":
                return customCyclicHashCode(key);
            case "ADDITIVE":
                return customAdditiveHashCode(key);
            default:
                throw new IllegalArgumentException("Unknown hashing strategy");
        }
    }
}
