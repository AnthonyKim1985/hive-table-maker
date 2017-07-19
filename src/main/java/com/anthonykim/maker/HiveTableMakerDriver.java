package com.anthonykim.maker;

import java.io.*;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-12.
 */
public class HiveTableMakerDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("java -jar HiveTableMaker.jar [database_name] [csv_directory]");
            System.exit(-1);
        }

        final String dbName = args[0];
        final String csvDirName = args[1];

        // 0: dbName, 1: csv dir
        runTableMaker(dbName, csvDirName);
    }

    private static void runTableMaker(String dbName, String csvDirName) throws Exception {
        File directory = new File(csvDirName);
        File[] fileList = directory.listFiles();

        assert fileList != null;
        for (File file : fileList) {
            if (file.exists()) {
                try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                     BufferedWriter tableWriter = new BufferedWriter(new FileWriter(new File("schema.txt"), true))) {
                    System.out.println(file.getName());

                    String header[] = bufferedReader.readLine().split("[,]");
                    int length = header.length;
                    boolean headerValidator[] = new boolean[length];

                    String tableName = file.getName().split("[.]")[0];
                    tableWriter.write(String.format("CREATE TABLE IF NOT EXISTS %s.%s (", dbName, tableName));

                    String line;
                    Pair pairs[] = new Pair[length];
                    while ((line = bufferedReader.readLine()) != null) {
                        String row[] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        //System.out.printf("row: %d, header: %d\n", row.length, header.length);
                        for (int i = 0; i < row.length; i++) {
                            String columnType = getDataType(row[i]);
                            if (!headerValidator[i])
                                if (columnType != null) {
                                    headerValidator[i] = true;
                                    pairs[i] = new Pair(header[i], columnType);
                                } else {
                                    pairs[i] = new Pair(header[i], "string");
                                }
                        }
                        if (isValidated(headerValidator))
                            break;
                    }

                    for (int i = 0; i < pairs.length; i++) {
                        Pair pair = pairs[i];
                        tableWriter.write(String.format("%s %s", pair.columnName, pair.columnType));

                        if (i < pairs.length - 1)
                            tableWriter.write(",");
                    }
                    tableWriter.write(String.format(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));
                }
            }
        }
    }

    private static boolean isValidated(boolean[] headerValidator) {
        for (boolean aHeaderValidator : headerValidator)
            if (!aHeaderValidator)
                return false;
        return true;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static String getDataType(String data) {
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

    private static class Pair {
        private String columnName;
        private String columnType;

        public Pair(String columnName, String columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType='" + columnType + '\'' +
                    '}';
        }
    }
}