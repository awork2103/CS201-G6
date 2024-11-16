package edu.smu.smusql;

import java.util.*;

public class HashMapPlusTree {
    private String tableName;
    private List<String> columns;
    // HashMap<ID, HashMap<ColumnName, CoumnValue>>
    private HashMap<String, HashMap<String, String>> table;
    public long initialmem;
    // TreeMap<ColumnValue, List<ID>>
    ArrayList<TreeMap<String, Set<String>>> tree = new ArrayList<>();

    public HashMapPlusTree(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns = columns;
        this.table = new HashMap<String, HashMap<String, String>>();
        initialmem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        for (int i = 0; i < columns.size(); i++) {
            tree.add(new TreeMap<String, Set<String>>(new TreeCustomComparator()));
        }
    }

    public long getMemConsumption() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() - initialmem;
    }

    public HashMap<String, HashMap<String, String>> getTable() {
        return table;
    }

    public void addEntry(HashMap<String, String> entry) {
        String id = entry.get("id");
        table.put(id, entry);

        // Add all column values to respective trees
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            String columnValue = entry.get(columnName);

            // Get the tree for the column
            TreeMap<String, Set<String>> columnTree = tree.get(i);

            // Get or create the set of IDs for the column value
            Set<String> ids = columnTree.getOrDefault(columnValue, new HashSet<>());
            ids.add(id);

            // Put the updated set back into the tree
            columnTree.put(columnValue, ids);
        }
    }

    public void deleteEntry(String id) {
        // Get the entry to be deleted
        HashMap<String, String> entry = table.remove(id);

        if (entry != null) {
            // Delete all column values from respective trees
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                String columnValue = entry.get(columnName);

                // Get the tree for the column
                TreeMap<String, Set<String>> columnTree = tree.get(i);

                // Get the set of IDs for the column value
                Set<String> ids = columnTree.get(columnValue);
                if (ids != null) {
                    ids.remove(id);
                    // If the set is empty, remove the entry from the tree
                    if (ids.isEmpty()) {
                        columnTree.remove(columnValue);
                    } else {
                        columnTree.put(columnValue, ids);
                    }
                }
            }
        }
    }

    public void updateEntry(String id, Map<String, String> updates) {
        // Retrieve the existing entry
        HashMap<String, String> existingEntry = table.get(id);
        if (existingEntry == null) {
            return;
        }

        // Update Trees whose columns have been updated
        for (Map.Entry<String, String> update : updates.entrySet()) {
            String columnName = update.getKey();
            String columnValue = update.getValue();

            // Get the tree for the column
            TreeMap<String, Set<String>> columnTree = getTree(columnName);

            // Search for id in Tree with old ColumnValue
            String oldColumnValue = existingEntry.get(columnName);
            Set<String> ids = columnTree.get(oldColumnValue);
            ids.remove(id);

            // Search for new ColumnValue and insert id into the set
            ids = columnTree.getOrDefault(columnValue, new HashSet<>());
            ids.add(id);
            columnTree.put(columnValue, ids);
        }

        // Update the HashMap with the new key-value pairs
        for (Map.Entry<String, String> update : updates.entrySet()) {
            existingEntry.put(update.getKey(), update.getValue());
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

    public TreeMap<String, Set<String>> getTree(String column) {
        return tree.get(columns.indexOf(column));
    }

    public Set<String> selectEntries(String[] condition) {
        if (condition == null) {
            return null;
        }

        Set<String> resultSet = new HashSet<>();
        if (condition[0] == null) {
            // {null, column, operator, value}
            TreeMap<String, Set<String>> tree = getTree(condition[1]);
            String value = condition[3];

            switch (condition[2]) {
                case "=":
                    if (tree.containsKey(value)) {
                        resultSet.addAll(tree.get(value));
                    }
                    break;
                case "<":
                    for (Map.Entry<String, Set<String>> entry : tree.headMap(value, false).entrySet()) {
                        resultSet.addAll(entry.getValue());
                    }
                    break;
                case ">":
                    for (Map.Entry<String, Set<String>> entry : tree.tailMap(value, false).entrySet()) {
                        resultSet.addAll(entry.getValue());
                    }
                    break;
                case "<=":
                    for (Map.Entry<String, Set<String>> entry : tree.headMap(value, true).entrySet()) {
                        resultSet.addAll(entry.getValue());
                    }
                    break;
                case ">=":
                    for (Map.Entry<String, Set<String>> entry : tree.tailMap(value, true).entrySet()) {
                        resultSet.addAll(entry.getValue());
                    }
                    break;
            }

        }
        return resultSet;
    }
}
