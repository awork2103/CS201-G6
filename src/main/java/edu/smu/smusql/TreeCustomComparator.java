package edu.smu.smusql;

import java.util.Comparator;

public class TreeCustomComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        boolean isNumeric1 = isNumeric(o1);
        boolean isNumeric2 = isNumeric(o2);

        if (isNumeric1 && isNumeric2) {
            Double num1 = Double.parseDouble(o1);
            Double num2 = Double.parseDouble(o2);
            return num1.compareTo(num2);
        } else {
            return o1.compareTo(o2);
        }
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
