package edu.smu.smusql;
import java.util.*;

public class CustomHashMapv2 {
    private String tableName;
    private List<String> columns;
    private HashMapv2<String, HashMapv2<String, String>> table;

    public CustomHashMapv2(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        // HashMapv2(int capacity, float loadFactor, int hashMultiplier, String hashingStrategy)
        this.table = new HashMapv2<String, HashMapv2<String, String>>(16, 0.75, 31, "ADDITIVE");
    }  

    public void addEntry(HashMapv2<String, String> entry) {
        table.put(entry.get("id"), entry);
    }


    public void deleteEntry(String id) {
        table.remove(id);
    }

    // Get Entry in table given a key (string => id)
    public HashMapv2<String,String> getEntry(String id) {
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
