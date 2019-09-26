/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCExample {

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      for (int i = 1; i <= 100; i++) {
        statement.execute("SET STORAGE GROUP TO root.sg" + i);
      }

      long startTime, endTime;
      startTime = System.currentTimeMillis();
      for (int i = 1; i <= 100; i++) {
        for (int j = 1; j <= 400; j++) {
          for (int k = 1; k <= 5; k++) {
            statement.execute("CREATE TIMESERIES root.sg" + i + ".d" + j + ".s" + k + " WITH DATATYPE=INT64, ENCODING=RLE");
          }
        }
      }
      endTime = System.currentTimeMillis();
      System.out.println("Time Consuming of CREATE TIMESERIES: " + (endTime - startTime) + " ms");

      startTime = System.currentTimeMillis();
      String insertStatement1, insertStatement2, insertStatement3;
      int a = 0;
      for (int i = 1; i <= 100; i++) {
        insertStatement3 = "insert into root.sg" + i;
        for (int j = 1; j <= 400; j++) {
          insertStatement2 = insertStatement3;
          insertStatement2 += ".d" + j + "(timestamp";
          for (int k = 1; k <= 5; k++) {
            insertStatement2 += ", s" + k;
          }
          insertStatement2 += ") values(";
          for (int k = 1; k <= 1000; k++) {
            insertStatement1 = insertStatement2;
            insertStatement1 += k;
            for (int l = 1; l <= 5; l++) {
              insertStatement1 += ", " + 1;
            }
            insertStatement1 += ")";
//            System.out.println(insertStatement1);
            a += 1;
            if (a % 10_000 == 0) {
              System.out.println(a);
            }
            statement.addBatch(insertStatement1);
            statement.executeBatch();
            statement.clearBatch();
          }
        }
      }
      endTime = System.currentTimeMillis();
      System.out.println("Time Consuming of INSERT INTO: " + (endTime - startTime) + " ms");
//      ResultSet resultSet = statement.executeQuery("select * from root where time <= 10");
//      outputResult(resultSet);
//      resultSet = statement.executeQuery("select count(*) from root");
//      outputResult(resultSet);
//      resultSet = statement.executeQuery("select count(*) from root where time >= 1 and time <= 100 group by (20ms, 0, [0, 100])");
//      outputResult(resultSet);
    }
  }

  private static void outputResult(ResultSet resultSet) throws SQLException {
    if (resultSet != null) {
      System.out.println("--------------------------");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        System.out.print(metaData.getColumnLabel(i + 1) + " ");
      }
      System.out.println();
      while (resultSet.next()) {
        for (int i = 1; ; i++) {
          System.out.print(resultSet.getString(i));
          if (i < columnCount) {
            System.out.print(", ");
          } else {
            System.out.println();
            break;
          }
        }
      }
      System.out.println("--------------------------\n");
    }
  }
}
