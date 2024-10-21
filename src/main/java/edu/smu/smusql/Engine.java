package edu.smu.smusql;

import java.util.*;

public class Engine {

    //Store the SQL Tables
    // Map<TableName, Table>
    // private Map<String, Table> tables = new HashMap<>();

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
        //TODO
        return "not implemented";
    }
    public String delete(String[] tokens) {
        //TODO
        return "not implemented";
    }

    // SELECT * from <table> where <column> <operator> <value>
    public String select(String[] tokens) {
        // Check SQL syntax
        if (!tokens[1].equals("*") || !tokens[2].toUpperCase().equals("FROM")) {
            return "ERROR: Invalid SELECT syntax";
        }

        // Get Table --> Return error if table does not exist
        String tableName = tokens[3];
        if (!tables.containsKey(tableName)) {
            return "ERROR: Table does not exist";
        }

        // Get Table and Cols
        Table table = tables.get(tableName);
        List<String> columns = table.getColumns();

        // Parse WHERE conditions
        List<String[]> whereConditions = parseWhereConditions(tokens);

        // StringBuilder to print result of Query
        StringBuilder result = new StringBuilder();
        result.append(String.join("\t", columns)).append("\n"); // Print column headers

        // TODO: Implement the logic to filter the rows that satisfy the WHERE conditions
        // HashMap Implementation
        Set<Integer> tableKeys = table.keySet();
        for (Integer key : tableKeys) {
            // Get table Entry from ID (key)
            Map<String, String> entry = table.get(key);
            boolean match = evaluateWhereConditions(entry, whereConditions);

            // Add row to result (to be printed in console)
            if (match) {
                for (String column : columns) {
                    result.append(entry.getOrDefault(column, "NULL")).append("\t");
                }
                result.append("\n");
            }
        }
    }
    
    public String update(String[] tokens) {
        //TODO
        return "not implemented";
    }
    public String create(String[] tokens) {
        //TODO
        return "not implemented";
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

    // Helper method to evaluate a single condition
    private boolean evaluateCondition(String columnValue, String operator, String value) {
        if (columnValue == null) return false;

        // Compare strings as numbers if possible
        boolean isNumeric = isNumeric(columnValue) && isNumeric(value);
        if (isNumeric) {
            double columnNumber = Double.parseDouble(columnValue);
            double valueNumber = Double.parseDouble(value);

            switch (operator) {
                case "=": return columnNumber == valueNumber;
                case ">": return columnNumber > valueNumber;
                case "<": return columnNumber < valueNumber;
                case ">=": return columnNumber >= valueNumber;
                case "<=": return columnNumber <= valueNumber;
            }
        } else {
            switch (operator) {
                case "=": return columnValue.equals(value);
                case ">": return columnValue.compareTo(value) > 0;
                case "<": return columnValue.compareTo(value) < 0;
                case ">=": return columnValue.compareTo(value) >= 0;
                case "<=": return columnValue.compareTo(value) <= 0;
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

}
