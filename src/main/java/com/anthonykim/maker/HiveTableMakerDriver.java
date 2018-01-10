package com.anthonykim.maker;

import com.anthonykim.maker.domain.ColumnPair;
import com.anthonykim.maker.util.TableMakerUtil;

import java.io.*;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-12.
 */
public class HiveTableMakerDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("java -jar HiveTableMaker.jar [database_name] [csv_directory] [delimiter: 1=comma, 2=tab'] [format: 1=text, 2=orc]");
            System.exit(-1);
        }

        final String dbName = args[0];
        final String csvDirName = args[1];
        final Integer delimiter = Integer.parseInt(args[2]);
        final Integer format = Integer.parseInt(args[3]);

        // 0: dbName, 1: csv dir
        runTableMaker(dbName, csvDirName, delimiter, format);
    }

    private static void runTableMaker(String dbName, String csvDirName, Integer delimiter, Integer format) throws Exception {
        File directory = new File(csvDirName);
        File[] fileList = directory.listFiles();

        assert fileList != null;
        for (File file : fileList) {
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                 BufferedWriter tableWriter = new BufferedWriter(new FileWriter(new File(String.format("schema_for_%s.txt", dbName)), true))) {
                StringBuilder ddlBuilder = new StringBuilder();

                System.out.println(file.getName());

                final String header[];
                if (delimiter == 1) {
                    header = bufferedReader.readLine().split("[,]");
                } else {
                    header = bufferedReader.readLine().split("[\t]");
                }

                int length = header.length;
                boolean headerValidator[] = new boolean[length];

                final String tableName;
                if (format == 1)
                    tableName = file.getName().split("[.]")[0] + "_text";
                else
                    tableName = file.getName().split("[.]")[0];

                ddlBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s (", dbName, tableName));

                String line;
                ColumnPair columnPairs[] = new ColumnPair[length];
                while ((line = bufferedReader.readLine()) != null) {
                    final String row[];
                    if (delimiter == 1) {
                        row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    } else {
                        row = line.split("\t", -1);
                    }
//                    System.out.printf("row: %d, header: %d\n", row.length, header.length);

                    for (int i = 0; i < row.length; i++) {
                        String columnType = TableMakerUtil.getDataType(row[i]);
                        if (columnType != null) {
                            if (columnPairs[i] == null)
                                columnPairs[i] = new ColumnPair("", "");

                            switch (columnType) {
                                case "int":
                                    if (columnPairs[i].getColumnType().isEmpty())
                                        columnPairs[i] = new ColumnPair(header[i], columnType);
                                    break;
                                case "double":
                                    if (columnPairs[i].getColumnType().equals("int"))
                                        columnPairs[i] = new ColumnPair(header[i], columnType);
                                    break;
                                case "string":
                                    if (!columnPairs[i].getColumnType().equals(columnType)) {
                                        columnPairs[i] = new ColumnPair(header[i], columnType);
                                        headerValidator[i] = true;
                                    }
                                    break;
                            }

                        } else {
                            columnPairs[i] = new ColumnPair(header[i], "string");
                        }
                    }

                    if (TableMakerUtil.isValidated(headerValidator))
                        break;
                }

                for (int i = 0; i < columnPairs.length; i++) {
                    ColumnPair columnPair = columnPairs[i];
                    ddlBuilder.append(String.format("%s %s", columnPair.getColumnName(), columnPair.getColumnType()));

                    if (i < columnPairs.length - 1)
                        ddlBuilder.append(",");
                }
                if (format == 1) {
                    if (delimiter == 1)
                        ddlBuilder.append(String.format(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));
                    else
                        ddlBuilder.append(String.format(") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));
                } else {
                    if (delimiter == 1)
                        ddlBuilder.append(String.format(") STORED AS ORC LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));
                    else
                        ddlBuilder.append(String.format(") STORED AS ORC LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));
                }


                tableWriter.write(ddlBuilder.toString());
                tableWriter.flush();
            }
        }
    }
}