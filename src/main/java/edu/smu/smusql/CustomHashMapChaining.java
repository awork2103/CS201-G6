package edu.smu.smusql;
import java.util.*;

public class CustomHashMapChaining {
    private String tableName;
    private List<String> columns;
    private SeparateChainingHashMap<String, SeparateChainingHashMap<String, String>> table;

    public CustomHashMapChaining(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        // SeparateChainingHashMap(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        // Types of hashing strategies: DEFAULT, BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
        this.table = new SeparateChainingHashMap<String, SeparateChainingHashMap<String, String>>(16, 0.75, 31, "BITWISE");
    }  

    public CustomHashMapChaining(String tableName, List<String> columns,String hashingStrategy) {
        this.tableName = tableName;
        this.columns = columns;
        // SeparateChainingHashMap(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        // Types of hashing strategies: DEFAULT, BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
        this.table = new SeparateChainingHashMap<String, SeparateChainingHashMap<String, String>>(16, 0.75, 31, hashingStrategy);
    }
    public CustomHashMapChaining(String tableName, List<String> columns,String hashingStrategy, double loadFactor) {
        this.tableName = tableName;
        this.columns = columns;
        // SeparateChainingHashMap(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        // Types of hashing strategies: DEFAULT, BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
        this.table = new SeparateChainingHashMap<String, SeparateChainingHashMap<String, String>>(16, loadFactor, 31, hashingStrategy);
    }  


    public void addEntry(SeparateChainingHashMap<String, String> entry) {
        table.put(entry.get("id"), entry);
    }


    public void deleteEntry(String id) {
        table.remove(id);
    }

    // Get Entry in table given a key (string => id)
    public SeparateChainingHashMap<String,String> getEntry(String id) {
        return table.get(id);
    }

    // Get all keys in table
    public Set<String> getKeys() {
        return table.keySet();
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }
}
