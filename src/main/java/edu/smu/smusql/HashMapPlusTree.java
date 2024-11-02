package edu.smu.smusql;

import java.util.*;

public class HashMapPlusTree {
    private String tableName;
    private List<String> columns;
    private HashMap<String, HashMap<String, String>> table;
    // TreeMap<ID, Colummn Value>
    ArrayList<TreeMap<String, String>> tree = new ArrayList<TreeMap<String, String>>();

    public HashMapPlusTree(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.table = new HashMap<String, HashMap<String, String>>();

        for (int i = 0; i < columns.size(); i++) {
            tree.add(new TreeMap<String, String>());
        }
    }  

    public HashMap<String, HashMap<String, String>> getTable() {
        return table;  
    }

    public void addEntry(HashMap<String, String> entry) {
        table.put(entry.get("ID"), entry);
        
        // Add all column values to respective trees
        for (int i = 0; i < columns.size(); i++) {
            // tree.get(i) --> get the tree for the column
            tree.get(i).put(entry.get("ID"), entry.get(columns.get(i)));
        }
    }

    public void deleteEntry(String id) {
        table.remove(id);

        // Delete all column values from respective trees
        for (int i = 0; i < columns.size(); i++) {
            tree.get(i).remove(id);
        }
    }

    public HashMap<String,String> getEntry(String id) {
        return table.get(id);
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public TreeMap<String, String> getTree(String column) {
        return tree.get(columns.indexOf(column));
    }
}
