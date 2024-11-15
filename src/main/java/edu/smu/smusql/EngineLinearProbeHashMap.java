package edu.smu.smusql;

import java.util.*;
import java.util.stream.IntStream;

public class EngineLinearProbeHashMap extends Engine {

    // Store the SQL Tables using CustomHashMapLinear
    private Map<String, CustomHashMapLinear> tables = new HashMap<>();

    @Override
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

        CustomHashMapLinear table = tables.get(tableName); // Assuming tables is a Map<String, CustomHashMap>
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
        LinearProbeHashMap<String, String> entry = new LinearProbeHashMap<>();
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


    @Override
    public String select(String[] tokens) {
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        String tableName = tokens[3];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        CustomHashMapLinear table = tables.get(tableName);
        List<String> columns = table.getColumns();
        List<String[]> whereConditions = parseWhereConditions(tokens);

        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n");

        for (String key : table.getKeys()) {
            LinearProbeHashMap<String, String> entry = table.getEntry(key);
            if (entry == null) continue;

            // Cast the entry to Map<String, String>
            boolean match = evaluateWhereConditions((Map<String, String>) entry, whereConditions);

            if (match) {
                for (String column : columns) {
                    result.append(entry.getOrDefault(column, "NULL")).append("\t");
                }
                result.append("\n");
            }
        }

        return result.toString();
    }

    @Override
    public String delete(String[] tokens) {
        if (!tokens[1].toUpperCase().equals("FROM") || tokens.length < 4 || !tokens[3].toUpperCase().equals("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }

        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        CustomHashMapLinear table = tables.get(tableName);
        List<String[]> whereConditions = parseDelete(tokens);

        if (whereConditions.isEmpty()) {
            return "ERROR: Invalid or unsupported WHERE conditions";
        }

        Set<String> idsToDelete = new HashSet<>();

        // Iterate over rows
        for (String key : table.getKeys()) {
            LinearProbeHashMap<String, String> entry = table.getEntry(key);
            if (entry == null) continue;

            boolean match = evaluateWhereConditions((Map<String, String>) entry, whereConditions);

            if (match) {
                idsToDelete.add(key);
            }
        }

        // Delete matching rows
        for (String id : idsToDelete) {
            table.deleteEntry(id);
        }

        return "SUCCESS: " + idsToDelete.size() + " row(s) deleted from " + tableName;
    }

    @Override
    public String update(String[] tokens) {
        if (!tokens[2].toUpperCase().equals("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }

        String tableName = tokens[1];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        CustomHashMapLinear table = tables.get(tableName);

        int setIndex = IntStream.range(0, tokens.length)
                                .filter(i -> tokens[i].equalsIgnoreCase("SET"))
                                .findFirst()
                                .orElse(-1);

        int whereIndex = IntStream.range(0, tokens.length)
                                  .filter(i -> tokens[i].equalsIgnoreCase("WHERE"))
                                  .findFirst()
                                  .orElse(-1);

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

        List<String[]> whereConditions = parseUpdate(tokens);

        int updatedCount = 0;

        for (String key : table.getKeys()) {
            LinearProbeHashMap<String, String> entry = table.getEntry(key);
            if (entry == null) continue;

            boolean match = evaluateWhereConditions((Map<String, String>) entry, whereConditions);

            if (match) {
                for (Map.Entry<String, String> update : updates.entrySet()) {
                    entry.put(update.getKey(), update.getValue());
                }
                updatedCount++;
            }
        }

        return "SUCCESS: " + updatedCount + " row(s) updated in " + tableName;
    }

    @Override
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

        CustomHashMapLinear newTable = new CustomHashMapLinear(tableName, columns);
        tables.put(tableName, newTable);

        return "Table " + tableName + " created successfully with columns: " + columns;
    }

    /*
     *  HELPER METHODS
     */
    // Helper method to extract content inside parentheses
    private String queryBetweenParentheses(String[] tokens, int startIndex) {
        StringBuilder result = new StringBuilder();
        for (int i = startIndex; i < tokens.length; i++) {
            result.append(tokens[i]).append(" ");
        }
        return result.toString().trim().replaceAll("\\(", "").replaceAll("\\)", "");
    }

    // Helper method too parse WHERE clauses
    @Override
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
            Double.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // Method to evaluate where conditions
    private boolean evaluateWhereConditions(Map<String, String> row, List<String[]> conditions) {
        boolean result = false;  // Default result should be false
        boolean currentCondition = true; // Tracks the result of the current condition
    
        // Track if an OR operator was encountered
        boolean previousConditionWasOR = false;
    
        for (String[] condition : conditions) {
            if (condition[0] != null) {  // Logical operator (AND/OR)
                if (condition[0].equals("AND")) {
                    // Apply AND logic: Combine currentCondition with result
                    result = result && currentCondition;
                    currentCondition = true; // Reset for the next condition
                } else if (condition[0].equals("OR")) {
                    // Apply OR logic: If the previous condition was OR, use OR
                    if (previousConditionWasOR) {
                        result = result || currentCondition;
                    } else {
                        result = currentCondition;
                    }
                    previousConditionWasOR = true;  // Mark that we encountered an OR
                    currentCondition = true;  // Reset for the next condition
                }
            } else {
                // Evaluate individual condition (column, operator, value)
                String column = condition[1];
                String operator = condition[2];
                String value = condition[3];
    
                currentCondition = evaluateCondition(row.get(column), operator, value);
            }
        }
    
        // After finishing evaluation, combine the final condition with result
        result = result || currentCondition;  // If it's OR, the last condition may still apply
    
        return result;
    }

    @Override
    public List<String[]> parseUpdate(String[] tokens) {
        List<String[]> whereClauseConditions = new ArrayList<>();
    
        int whereIndex = IntStream.range(0, tokens.length)
                          .filter(i -> tokens[i].equalsIgnoreCase("WHERE"))
                          .findFirst()
                          .orElse(-1);

        if (whereIndex != -1) {
            for (int i = whereIndex + 1; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("AND") || tokens[i].equalsIgnoreCase("OR")) {
                    // Logical operators
                    whereClauseConditions.add(new String[]{tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    // Conditions (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[]{null, column, operator, value});
                    i++; // Skip the value since it has been processed
                }
            }
        }
    
        return whereClauseConditions;
    }

    @Override
    public List<String[]> parseDelete(String[] tokens) {

        List<String[]> whereClauseConditions = new ArrayList<>(); // Array for storing conditions from the where clause.

        // Parse WHERE clause conditions
        if (tokens.length > 3 && tokens[3].toUpperCase().equals("WHERE")) {
            for (int i = 4; i < tokens.length; i++) {
                if (tokens[i].toUpperCase().equals("AND") || tokens[i].toUpperCase().equals("OR")) {
                    // Add AND/OR conditions
                    whereClauseConditions.add(new String[] {tokens[i].toUpperCase(), null, null, null});
                } else if (isOperator(tokens[i])) {
                    // Add condition with operator (column, operator, value)
                    String column = tokens[i - 1];
                    String operator = tokens[i];
                    String value = tokens[i + 1];
                    whereClauseConditions.add(new String[] {null, column, operator, value});
                    i += 1; // Skip the value since it has been processed
                }
            }
        }
        return whereClauseConditions;
    }

}
