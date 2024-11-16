package edu.smu.smusql;
import edu.smu.smusql.Testing;
import edu.smu.smusql.helperResults;
import java.util.*;

public class Results {

    int hashMultiplier = 31;

    private static final long MEGABYTE = 1024L * 1024L;
    private static final long nano = 1000000000;

    public static double nanosecondsToSeconds(double nanoSeconds){
        return nanoSeconds / nano;
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static long getMem(){
        return Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
    }

    public static double getTime(){
        return nanosecondsToSeconds(System.nanoTime());
    }

    public void testHashFunction(){

        String[] toTest = {"BITWISE", "ADDITIVE", "POLYNOMIAL", "CYCLIC"};

        String hashValue = "hashtest";

        for (String hashCode : toTest){

            System.out.println(hashCode);

            long beforeUsedMem=getMem();
            double start = getTime();

            for (int i = 0; i < 10000; i++){
                customHashCode( hashValue, hashCode);

            }

            double end = getTime();
            long afterUsedMem=getMem();
            long actualMemUsed=bytesToMegabytes(afterUsedMem - beforeUsedMem);
            double timeTaken = nanosecondsToSeconds(end - start);

            //System.out.println("Time elapsed    :" + timeTaken + " seconds");
            System.out.println("Individual time :" + timeTaken/10000 + " seconds");
            System.out.println("Memory used " + actualMemUsed + " MB");
            System.out.println();

            System.gc(); 
            
        }
        
    }

    public void testHashMapWithTreesComparison(){
        Engine treeEngine = new EngineHashMapPlusTree();
        Engine normalEngine = new Engine();
        long numberOfInsertions = 15000;

        double treeEngineInsertionTime;
        double EngineInsertionTime;

        for (int i = 0; i < 5; )
        
        helperResults.prepopulateUserTable(treeEngine, new Random(), )
    }

    
    
    public static void main(String[] args) {

        Results testingobj = new Results();
        
        testingobj.testHashFunction();

        
    }

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
