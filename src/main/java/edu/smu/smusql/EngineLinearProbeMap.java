package edu.smu.smusql;

import java.util.*;

public class EngineLinearProbeMap extends Engine{
/* 
    // Store the SQL Tables
    // engine for the LinearProbeHashMap
    private Map<String, LinearProbeHashMap> tables = new HashMap<>();

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

    public String insert(String[] tokens) {
        // Basic syntax check
        if (!tokens[1].toUpperCase().equals("INTO")) {
            return "ERROR: Invalid INSERT syntax";
        }

        // Get Table name and ensure it exists
        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        LinearProbeHashMap table = tables.get(tableName); // Assuming tables is a Map<String, LinearProbeHashMap>
        List<String> columns = table.getColumns();

        // Parse column names and values
        String[] columnNames = queryBetweenParentheses(tokens, 3).split(","); // Get column names between ( )
        int valuesIndex = Arrays.asList(tokens).indexOf("VALUES");
        if (valuesIndex == -1) {
            return "ERROR: Missing VALUES clause";
        }

        String[] values = queryBetweenParentheses(tokens, valuesIndex + 1).split(","); // Get values between ( )
        if (columnNames.length != values.length) {
            return "ERROR: Number of columns does not match number of values";
        }

        // Create the entry to insert
        HashMap<String, String> entry = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            entry.put(columns.get(i), values[i].trim());
        }

        // Ensure the ID is provided
        if (!entry.containsKey("id")) {
            return "ERROR: Entry must have an ID";
        }

        // Insert the entry into the table
        table.addEntry(entry);

        return "SUCCESS: Row inserted into " + tableName;
    }

    public String delete(String[] tokens) {
        // Check SQL syntax: DELETE FROM <table> WHERE <column> <operator> <value>
        if (!tokens[1].toUpperCase().equals("FROM") || !tokens[3].toUpperCase().equals("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }

        // Get the table name
        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        // Get the table object
        LinearProbeHashMap table = tables.get(tableName);

        // Get the column, operator, and value from the WHERE clause
        String column = tokens[4];
        String operator = tokens[5];
        String value = tokens[6];

        // Check if the operator is valid (e.g., =, <, >, <=, >=)
        if (!isOperator(operator)) {
            return "ERROR: Unsupported operator in DELETE statement";
        }

        // Handle the case where the column is "ID" and operator is "="
        if (column.equalsIgnoreCase("ID") && operator.equals("=")) {
            table.deleteEntry(value); // Directly delete entry by ID
            return "Entry with ID " + value + " deleted.";
        } else {
            // Handle the case where we need to evaluate conditions
            Set<String> idsToDelete = new HashSet<>();

            // Loop through the table to find matching entries
            for (String id : table.getKeys()) {
                HashMap<String, String> entry = table.getEntry(id);
                if (entry != null) {
                    String columnValue = entry.get(column);

                    if (evaluateCondition(columnValue, operator, value)) {
                        idsToDelete.add(id); // Collect IDs that match the WHERE condition
                    }
                }
            }

            // Delete all matching entries
            for (String id : idsToDelete) {
                table.deleteEntry(id);
            }

            return idsToDelete.size() + " entries deleted.";
        }
    }

    // SELECT * from <table> where <column> <operator> <value>
    public String select(String[] tokens) {
        // Check SQL syntax: SELECT * FROM <table> WHERE <conditions>
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        // Get table name
        String tableName = tokens[3];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        // Get the table object
        LinearProbeHashMap table = tables.get(tableName);
        List<String> columns = table.getColumns(); // All columns in the table

        // Parse WHERE conditions (if present)
        List<String[]> whereConditions = parseWhereConditions(tokens);

        // StringBuilder to print the result of the query
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers

        // Iterate through the table entries (each entry represents a row)
        for (String key : table.getKeys()) {
            // Get the entry (row) by ID
            Map<String, String> entry = table.getEntry(key);
            if (entry == null) {
                continue;
            }

            // Evaluate the WHERE conditions for this entry
            boolean match = evaluateWhereConditions(entry, whereConditions);

            // If the row matches the conditions, add it to the result
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
        // Check SQL syntax: UPDATE <table> SET <column1> = <value1>, ... WHERE
        // <conditions>
        if (!tokens[2].toUpperCase().equals("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        // Get the table name
        String tableName = tokens[1];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        // Get the table object
        LinearProbeHashMap table = tables.get(tableName);

        // Parse the SET clause (the list of columns and values to update)
        int setIndex = Arrays.asList(tokens).indexOf("SET");
        int whereIndex = Arrays.asList(tokens).indexOf("WHERE");

        // Ensure WHERE clause exists
        if (whereIndex == -1) {
            return "ERROR: Missing WHERE clause";
        }

        // Parse the column-value pairs for the SET clause
        String[] setClauses = Arrays.copyOfRange(tokens, setIndex + 1, whereIndex);
        Map<String, String> updates = new HashMap<>();

        // Extract column-value pairs (e.g., column1 = value1, column2 = value2)
        for (int i = 0; i < setClauses.length; i += 3) {
            String column = setClauses[i];
            String operator = setClauses[i + 1];
            String value = setClauses[i + 2].replace(",", ""); // Remove any trailing commas

            if (!operator.equals("=")) {
                return "ERROR: Invalid operator in SET clause";
            }

            // Ensure the column exists in the table
            if (!table.getColumns().contains(column)) {
                return "ERROR: Column " + column + " does not exist in table " + tableName;
            }

            // Add the update to the map
            updates.put(column, value);
        }

        // Parse WHERE conditions, including inequalities
        List<String[]> whereConditions = parseWhereConditions(tokens);

        // Update the matching entries in the table
        int updatedCount = 0;
        for (String key : table.getKeys()) {
            Map<String, String> entry = table.getEntry(key);
            if (entry == null) {
                continue;
            }

            // Evaluate WHERE conditions for this entry
            boolean match = evaluateWhereConditions(entry, whereConditions);

            // If the entry matches the WHERE conditions, apply the updates
            if (match) {
                for (Map.Entry<String, String> update : updates.entrySet()) {
                    entry.put(update.getKey(), update.getValue());
                }
                updatedCount++;
            }
        }

        return "SUCCESS: " + updatedCount + " row(s) updated in " + tableName;
    }

    public String create(String[] tokens) {

        if (!tokens[1].equalsIgnoreCase("TABLE")) {
            return "ERROR: Invalid CREATE TABLE syntax";
        }

        String tableName = tokens[2];
        if (tables.containsKey(tableName)) {
            return "ERROR: Table already exists";
        }

        String columnList = queryBetweenParentheses(tokens, 3); // Get column list between parentheses

        if (columnList == null || columnList.isEmpty()) {
            return "ERROR: No columns specified";
        }

        List<String> columns = Arrays.asList(columnList.split(","));

        // Trim each column name to avoid spaces around them
        columns.replaceAll(String::trim);

        LinearProbeHashMap newTable = new LinearProbeHashMap(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created successfully with columns: " + columns;
    }

    /*
     * HELPER METHODS
    *//* 
    // Helper method to extract content inside parentheses
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
                    whereClauseConditions.add(new String[] { tokens[i].toUpperCase(), null, null, null });
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[] { null, column, operator, value });
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
    } */

}
