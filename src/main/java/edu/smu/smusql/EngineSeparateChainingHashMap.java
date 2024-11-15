package edu.smu.smusql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
//import edu.smu.smusql.SeparateChainingHashMap;    
import java.util.stream.IntStream;

public class EngineSeparateChainingHashMap extends Engine{
    //Store the SQL Tables
    // engine for the CustomHashMapChaining
    private Map<String, CustomHashMapChaining> tables = new HashMap<>();
    private String hashingStrategy  = "DEFAULT";
    private double loadFactor = 0.75;

    public EngineSeparateChainingHashMap(){
    }

    public EngineSeparateChainingHashMap(String hashingStrategy){
        this.hashingStrategy = hashingStrategy; 
    }
    public EngineSeparateChainingHashMap(double loadFactor){
        this.loadFactor = loadFactor; 
    }

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

        CustomHashMapChaining table = tables.get(tableName); // Assuming tables is a Map<String, CustomHashMapChaining>
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
        SeparateChainingHashMap<String, String> entry = new SeparateChainingHashMap<>(columnNames.length, 1, 2, "BITWISE");
        for (int i = 0; i < columnNames.length; i++) {
            entry.put(columns.get(i).trim(), values[i].trim());
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
        // Validate syntax: Ensure "FROM" and "WHERE" are present
        if (!tokens[1].toUpperCase().equals("FROM") || tokens.length < 4 || !tokens[3].toUpperCase().equals("WHERE")) {
            return "ERROR: Invalid DELETE syntax";
        }
    
        String tableName = tokens[2];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }
    
        // Get the table object
        CustomHashMapChaining table = tables.get(tableName);
    
        // Parse WHERE conditions using parseDelete (similar to the second version)
        List<String[]> whereConditions = parseDelete(tokens);
    
        if (whereConditions.isEmpty()) {
            return "ERROR: Invalid or unsupported WHERE conditions";
        }
    
        // Set to store IDs of rows to delete
        Set<String> idsToDelete = new HashSet<>();
    
        // Iterate over the rows in the table
        for (String id : table.getKeys()) {
            SeparateChainingHashMap<String, String> entry = table.getEntry(id);
            if (entry == null) continue; // Skip if entry is null
    
            // Evaluate if entry matches all the WHERE conditions
            boolean match = evaluateWhereConditions(entry, whereConditions);
    
            if (match) {
                idsToDelete.add(id); // Add matching row ID to the set
            }
        }
    
        // Delete all matching rows
        for (String id : idsToDelete) {
            table.deleteEntry(id);
        }
    
        // Return the result
        return "SUCCESS: " + idsToDelete.size() + " row(s) deleted from " + tableName;
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
        CustomHashMapChaining table = tables.get(tableName);
        List<String> columns = table.getColumns(); // All columns in the table

        // Parse WHERE conditions (if present)
        List<String[]> whereConditions = parseWhereConditions(tokens);

        // StringBuilder to print the result of the query
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers

        // Iterate through the table entries (each entry represents a row)
        for (String key : table.getKeys()) {
            // Get the entry (row) by ID
            SeparateChainingHashMap<String, String> entry = table.getEntry(key);
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
        // Validate syntax: Ensure "SET" is present and properly positioned
        if (!tokens[2].toUpperCase().equals("SET")) {
            return "ERROR: Invalid UPDATE syntax";
        }
    
        String tableName = tokens[1];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }
    
        // Get the table object
        CustomHashMapChaining table = tables.get(tableName);
    
        // Find indices of SET and WHERE using IntStream for flexibility
        int setIndex = IntStream.range(0, tokens.length)
                                .filter(i -> tokens[i].equalsIgnoreCase("SET"))
                                .findFirst()
                                .orElse(-1);
    
        int whereIndex = IntStream.range(0, tokens.length)
                                  .filter(i -> tokens[i].equalsIgnoreCase("WHERE"))
                                  .findFirst()
                                  .orElse(-1);
    
        // Ensure WHERE clause exists
        if (whereIndex == -1) {
            return "ERROR: Missing WHERE clause";
        }
    
        // Parse the SET clauses (the column-value pairs to update)
        String[] setClauses = Arrays.copyOfRange(tokens, setIndex + 1, whereIndex);
        Map<String, String> updates = new HashMap<>();
    
        for (int i = 0; i < setClauses.length; i += 3) {
            String column = setClauses[i];
            String operator = setClauses[i + 1];
            String value = setClauses[i + 2].replace(",", ""); // Remove any trailing commas
    
            // Check if the operator is valid
            if (!operator.equals("=")) {
                return "ERROR: Invalid operator in SET clause";
            }
    
            // Ensure the column exists in the table
            if (!table.getColumns().contains(column)) {
                return "ERROR: Column " + column + " does not exist in table " + tableName;
            }
    
            // Add the column-value pair to the updates map
            updates.put(column, value);
        }
    
        // Parse WHERE conditions using parseUpdate (for conditions on which rows to update)
        List<String[]> whereConditions = parseUpdate(tokens);
    
        int updatedCount = 0;
    
        // Iterate over rows and apply updates where conditions are met
        for (String key : table.getKeys()) {
            Map<String, String> entry = table.getEntry(key);
            if (entry == null) continue;
    
            // Check if the current entry matches the WHERE conditions
            boolean match = evaluateWhereConditions(entry, whereConditions);
    
            if (match) {
                // Apply updates to the matching entry
                for (Map.Entry<String, String> update : updates.entrySet()) {
                    entry.put(update.getKey(), update.getValue());
                }
                updatedCount++; // Increment the updated row count
            }
        }
    
        // Return the result of the update operation
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

        CustomHashMapChaining newTable = new CustomHashMapChaining(tableName, columns, hashingStrategy, loadFactor);
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
                    i += 1; //  Skip the value since it has been processed
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
