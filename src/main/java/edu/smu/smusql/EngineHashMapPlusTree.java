package edu.smu.smusql;

import java.util.*;

public class EngineHashMapPlusTree extends Engine{

    private Map<String, HashMapPlusTree> tables = new HashMap<>();

    public String executeSQL(String query) {
        String[] tokens = query.trim().split("\\s+");
        String command = tokens[0].toUpperCase();

        switch (command) {
            case "CREATE":
                return create(tokens);
            case "INSERT":
                return insert(tokens);
            case "SELECT":
                return select(tokens);
            case "UPDATE":
                return update(tokens);
            case "DELETE":
                return delete(tokens);
            default:
                return "ERROR: Unknown command";
        }
    }

    public String create(String[] tokens) {
        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        if (tables.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        String columnList = queryBetweenParentheses(tokens, 3);
        if (columnList == null || columnList.isEmpty()) {
            return "ERROR: No columns specified";
        }

        List<String> columns = Arrays.asList(columnList.split(","));
        columns.replaceAll(String::trim);

        HashMapPlusTree newTable = new HashMapPlusTree(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created successfully with columns: " + columns;
    }

    public String insert(String[] tokens) {
        if (!tokens[1].toUpperCase().equals("INTO")) {
            return "ERROR: Invalid INSERT syntax";
        }

        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        HashMapPlusTree table = tables.get(tableName);
        List<String> columns = table.getColumns();

        String[] columnNames = queryBetweenParentheses(tokens, 3).split(",");
        int valuesIndex = Arrays.asList(tokens).indexOf("VALUES");
        if (valuesIndex == -1) {
            return "ERROR: Missing VALUES clause";
        }

        String[] values = queryBetweenParentheses(tokens, valuesIndex + 1).split(",");
        if (columnNames.length != values.length) {
            return "ERROR: Number of columns does not match number of values";
        }

        HashMap<String, String> entry = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            entry.put(columnNames[i].trim(), values[i].trim());
        }

        if (!entry.containsKey("ID")) {
            return "ERROR: Entry must have an ID";
        }

        table.addEntry(entry);

        return "SUCCESS: Row inserted into " + tableName;
    }

    public String delete(String[] tokens) {
        if (!tokens[1].toUpperCase().equals("FROM") || !tokens[3].toUpperCase().equals("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        HashMapPlusTree table = tables.get(tableName);

        String column = tokens[4];
        String operator = tokens[5];
        String value = tokens[6];

        if (!isOperator(operator)) {
            return "ERROR: Unsupported operator in DELETE statement";
        }

        if (column.equalsIgnoreCase("ID") && operator.equals("=")) {
            table.deleteEntry(value);
            return "Entry with ID " + value + " deleted.";
        } else {
            Set<String> idsToDelete = new HashSet<>();
            TreeMap<String, String> columnTree = table.getTree(column);

            switch (operator) {
                case "=" -> idsToDelete.addAll(columnTree.entrySet().stream()
                    .filter(e -> e.getValue().equals(value))
                    .map(Map.Entry::getKey)
                    .toList());
                case ">" -> idsToDelete.addAll(columnTree.tailMap(value, false).keySet());
                case ">=" -> idsToDelete.addAll(columnTree.tailMap(value, true).keySet());
                case "<" -> idsToDelete.addAll(columnTree.headMap(value, false).keySet());
                case "<=" -> idsToDelete.addAll(columnTree.headMap(value, true).keySet());
            }

            for (String id : idsToDelete) {
                table.deleteEntry(id);
            }

            return idsToDelete.size() + " entries deleted.";
        }
    }

    public String select(String[] tokens) {
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        String tableName = tokens[3];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        HashMapPlusTree table = tables.get(tableName);
        List<String> columns = table.getColumns();

        List<String[]> whereConditions = parseWhereConditions(tokens);

        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n");

        for (String key : table.getTable().keySet()) {
            Map<String, String> entry = table.getEntry(key);
            if (entry == null) continue;

            boolean match = evaluateWhereConditions(entry, whereConditions);

            if (match) {
                for (String column : columns) {
                    result.append(entry.getOrDefault(column, "NULL")).append("\t");
                }
                result.append("\n");
            }
        }

        return result.toString();
    }

    public String update(String[] tokens) {
        if (!tokens[2].toUpperCase().equals("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        String tableName = tokens[1];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        HashMapPlusTree table = tables.get(tableName);

        int setIndex = Arrays.asList(tokens).indexOf("SET");
        int whereIndex = Arrays.asList(tokens).indexOf("WHERE");

        if (whereIndex == -1) {
            return "ERROR: Missing WHERE clause";
        }

        String[] setClauses = Arrays.copyOfRange(tokens, setIndex + 1, whereIndex);
        Map<String, String> updates = new HashMap<>();

        for (int i = 0; i < setClauses.length; i += 3) {
            String column = setClauses[i];
            String operator = setClauses[i + 1];
            String value = setClauses[i + 2].replace(",", "");

            if (!operator.equals("=")) {
                return "ERROR: Invalid operator in SET clause";
            }

            if (!table.getColumns().contains(column)) {
                return "ERROR: Column " + column + " does not exist in table " + tableName;
            }

            updates.put(column, value);
        }

        List<String[]> whereConditions = parseWhereConditions(tokens);

        int updatedCount = 0;
        for (String key : table.getTable().keySet()) {
            Map<String, String> entry = table.getEntry(key);
            if (entry == null) continue;

            boolean match = evaluateWhereConditions(entry, whereConditions);

            if (match) {
                for (Map.Entry<String, String> update : updates.entrySet()) {
                    entry.put(update.getKey(), update.getValue());
                }
                updatedCount++;
            }
        }

        return "SUCCESS: " + updatedCount + " row(s) updated in " + tableName;
    }

    // Helper methods are the same as in the previous version
    // queryBetweenParentheses, parseWhereConditions, evaluateCondition, etc.

    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

    // Helper method too parse WHERE clauses
    public List<String[]> parseWhereConditions(String[] tokens) {
        List<String[]> whereClauseConditions = new ArrayList<>();

        if (tokens.length > 4 && tokens[4].equalsIgnoreCase("WHERE")) {
            for (int i = 5; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    // Add AND/OR conditions
                    whereClauseConditions.add(new String[]{tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[]{null, column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }
        return whereClauseConditions;
    }

    // Helper method to evaluate a single condition
    private boolean evaluateCondition(String columnValue, String operator, String value) {
        if (columnValue == null) {
            return false;
        }

        // Compare strings as numbers if possible
        boolean isNumeric = isNumeric(columnValue) && isNumeric(value);
        if (isNumeric) {
            double columnNumber = Double.parseDouble(columnValue);
            double valueNumber = Double.parseDouble(value);

            switch (operator) {
                case "=":
                    return columnNumber == valueNumber;
                case ">":
                    return columnNumber > valueNumber;
                case "<":
                    return columnNumber < valueNumber;
                case ">=":
                    return columnNumber >= valueNumber;
                case "<=":
                    return columnNumber <= valueNumber;
            }
        } else {
            switch (operator) {
                case "=":
                    return columnValue.equals(value);
                case ">":
                    return columnValue.compareTo(value) > 0;
                case "<":
                    return columnValue.compareTo(value) < 0;
                case ">=":
                    return columnValue.compareTo(value) >= 0;
                case "<=":
                    return columnValue.compareTo(value) <= 0;
            }
        }

        return false;
    }

    // Helper method to determine if a string is an operator
    private boolean isOperator(String token) {
        return token.equals("=") || token.equals(">") || token.equals("<") || token.equals(">=") || token.equals("<=");
    }

    // Helper method to determine if a string is numeric
    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Method to evaluate where conditions
    private boolean evaluateWhereConditions(Map<String, String> row, List<String[]> conditions) {
        boolean overallMatch = true;
        boolean nextConditionShouldMatch = true; // Default behavior for AND

        for (String[] condition : conditions) {
            if (condition[0] != null) { // AND/OR operator
                nextConditionShouldMatch = condition[0].equals("AND");
            } else {
                // Parse column, operator, and value
                String column = condition[1];
                String operator = condition[2];
                String value = condition[3];

                boolean currentMatch = evaluateCondition(row.get(column), operator, value);

                if (nextConditionShouldMatch) {
                    overallMatch = overallMatch && currentMatch;
                } else {
                    overallMatch = overallMatch || currentMatch;
                }
            }
        }

        return overallMatch;
    }



    // Helper method to access table data from HashMapPlusTree
    private Map<String, HashMap<String, String>> getTableData(HashMapPlusTree table) {
        return table.getTable();
    }









}