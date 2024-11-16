package edu.smu.smusql;

import java.util.*;

public class CustomHashMap {
    private String tableName;
    private List<String> columns;
    private HashMap<String, HashMap<String, String>> table;
    public long initialmem;

    public CustomHashMap(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.table = new HashMap<String, HashMap<String, String>>();
        initialmem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
    }  

    public void addEntry(HashMap<String, String> entry) {
        table.put(entry.get("id"), entry);
    }

    public long getMemConsumption(){
        return Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory() - initialmem;
    }


    public void deleteEntry(String id) {
        table.remove(id);
    }

    // Get Entry in table given a key (string => id)
    public HashMap<String,String> getEntry(String id) {
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
