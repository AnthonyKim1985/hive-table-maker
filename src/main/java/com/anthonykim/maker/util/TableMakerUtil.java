package com.anthonykim.maker.util;

/**
 * Created by Anthony Jinhyuk Kim on 2017-07-19.
 */
public class TableMakerUtil {
    public static boolean isValidated(boolean[] headerValidator) {
        for (boolean aHeaderValidator : headerValidator)
            if (!aHeaderValidator)
                return false;
        return true;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static String getDataType(String data) {
        data = data.trim();
        if (data.isEmpty())
            return null;
        try {
            Integer.parseInt(data);
            return "int";
        } catch (NumberFormatException e1) {
            try {
                Double.parseDouble(data);
                return "double";
            } catch (NumberFormatException e2) {
                return "string";
            }
        }
    }
}
