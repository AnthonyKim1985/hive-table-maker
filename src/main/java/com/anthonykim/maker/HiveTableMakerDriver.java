package com.anthonykim.maker;

import com.anthonykim.maker.domain.ColumnPair;
import com.anthonykim.maker.util.TableMakerUtil;

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
            try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                 BufferedWriter tableWriter = new BufferedWriter(new FileWriter(new File("schema.txt"), true))) {
                StringBuilder ddlBuilder = new StringBuilder();

                System.out.println(file.getName());

                String header[] = bufferedReader.readLine().split("[,]");
                int length = header.length;
                boolean headerValidator[] = new boolean[length];

                String tableName = file.getName().split("[.]")[0];
                ddlBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s.%s (", dbName, tableName));

                String line;
                ColumnPair columnPairs[] = new ColumnPair[length];
                while ((line = bufferedReader.readLine()) != null) {
                    String row[] = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    //System.out.printf("row: %d, header: %d\n", row.length, header.length);
                    for (int i = 0; i < row.length; i++) {
                        String columnType = TableMakerUtil.getDataType(row[i]);
                        if (!headerValidator[i])
                            if (columnType != null) {
                                headerValidator[i] = true;
                                columnPairs[i] = new ColumnPair(header[i], columnType);
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
                ddlBuilder.append(String.format(") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' LOCATION '/user/hive/warehouse/%s.db/%s';\n", dbName, tableName));

                tableWriter.write(ddlBuilder.toString());
                tableWriter.flush();
            }
        }
    }

}