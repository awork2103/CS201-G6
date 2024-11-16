package edu.smu.smusql;

import java.util.*;

public class HashMapPlusTree {
    private String tableName;
    private List<String> columns;
    // HashMap<ID, HashMap<ColumnName, Tree>>
    private HashMap<String, HashMap<String, String>> table;
    public long initialmem;
    // TreeMap<ID, Colummn Value>
    ArrayList<TreeMap<String, String>> tree = new ArrayList<TreeMap<String, String>>();

    public HashMapPlusTree(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.table = new HashMap<String, HashMap<String, String>>();
        initialmem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < columns.size(); i++) {
            tree.add(new TreeMap<String, String>());
        }
    }

    public long getMemConsumption() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() - initialmem;
    }

    public HashMap<String, HashMap<String, String>> getTable() {
        return table;
    }

    public void addEntry(HashMap<String, String> entry) {
        table.put(entry.get("id"), entry);

        // Add all column values to respective trees
        for (int i = 0; i < columns.size(); i++) {
            // tree.get(i) --> get the tree for the column
            tree.get(i).put(entry.get("id"), entry.get(columns.get(i)));
        }
    }

    public void deleteEntry(String id) {
        table.remove(id);

        // Delete all column values from respective trees
        for (int i = 0; i < columns.size(); i++) {
            tree.get(i).remove(id);
        }
    }

    public HashMap<String, String> getEntry(String id) {
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

    public Set<String> selectEntries(String[] condition) {
        if (condition[0] == null) {
            
            // {null, column, operator, value}
            TreeMap<String, String> tree = getTree(condition[1]);
            String value = condition[3];
            boolean isNumeric;

            try {
                Integer.parseInt(value);
                isNumeric = true;
            } catch (NumberFormatException e) {
                isNumeric = false;
            }

            if (isNumeric) {
                int numericValue = Integer.parseInt(value);
                switch (condition[2]) {
                    case "=":
                        return tree.subMap(value, true, value, true).keySet();
                    case "<":
                        return tree.headMap(String.valueOf(numericValue), false).keySet();
                    case ">":
                        return tree.tailMap(String.valueOf(numericValue), false).keySet();
                    case "<=":
                        return tree.headMap(String.valueOf(numericValue), true).keySet();
                    case ">=":
                        return tree.tailMap(String.valueOf(numericValue), true).keySet();
                    default:
                        return null;
                }
            } else {
                switch (condition[2]) {
                    case "=":
                        return tree.subMap(value, true, value, true).keySet();
                    case "<":
                        return tree.headMap(value, false).keySet();
                    case ">":
                        return tree.tailMap(value, false).keySet();
                    case "<=":
                        return tree.headMap(value, true).keySet();
                    case ">=":
                        return tree.tailMap(value, true).keySet();
                    default:
                        return null;
                }
            }
        }
        return null;
    }
}
