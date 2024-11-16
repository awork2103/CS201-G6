package edu.smu.smusql;

import java.util.*;

public class CustomHashMapLinear {
    private String tableName;
    private List<String> columns;
    private LinearProbeHashMap<String, LinearProbeHashMap<String, String>> table;

    public CustomHashMapLinear(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        // LinearProbeHashMap(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        // Types of hashing strategies: DEFAULT, BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
        this.table = new LinearProbeHashMap<String, LinearProbeHashMap<String, String>>(16, 0.75, 31, "DEFAULT");
    }  

    public CustomHashMapLinear(String tableName, List<String> columns, double loadFactor) {
        this.tableName = tableName;
        this.columns = columns;
        // LinearProbeHashMap(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        // Types of hashing strategies: DEFAULT, BITWISE, POLYNOMIAL, CYCLIC, ADDITIVE
        this.table = new LinearProbeHashMap<String, LinearProbeHashMap<String, String>>(16, loadFactor, 31, "DEFAULT");
    }  

    public void addEntry(LinearProbeHashMap<String, String> entry) {
        table.put(entry.get("id"), entry);
    }


    public void deleteEntry(String id) {
        table.remove(id);
    }

    // Get Entry in table given a key (string => id)
    public LinearProbeHashMap<String,String> getEntry(String id) {
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
