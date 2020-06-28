/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.*;
import java.util.Map;
import java.util.Set;

import org.vanilladb.core.remote.jdbc.JdbcDriver;

public class ConsoleSQLInterpreter {
    private static Connection conn = null;
    
    public static void main(String[] args) {
        try {
            Driver d = new JdbcDriver();
            conn = d.connect("jdbc:vanilladb://localhost", null);
            
            Reader rdr = new InputStreamReader(System.in);
            BufferedReader br = new BufferedReader(rdr);
            
            while (true) {
                // process one line of input
                System.out.print("\nSQL> ");
                String cmd = br.readLine().trim();
                System.out.println();
                
                if (cmd.trim().length() == 0) {
                    System.out.println("你输入的命令为空！请重新输入！");
                    continue;
                }
                String cmdArray[] = cmd.trim().split(";");// many cmd
                
                for (String cmdOne : cmdArray) {
                    String[] str = cmdOne.split("\\s+");
                    String cmdf = str[0].toUpperCase();
                    
                    if (cmd.startsWith("exit") || cmd.startsWith("EXIT"))
                        break;
                    else if (cmdf.startsWith("SHOW")) {
                        if (str.length == 2 && "TABLES".equalsIgnoreCase(str[1])) {
                            doShowTables(cmdOne);
                        }
                    } else if (cmdf.startsWith("DESC")) {
                        String tableName = str[1].toLowerCase();
                        doDescribe(cmdOne, tableName);
                    } else if (cmdf.startsWith("SELECT") || cmdf.startsWith("EXPLAIN"))
                        doQuery(cmd, cmdf);
                    else
                        doUpdate(cmd);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private static void doShowTables(String cmdOne) {
        ResultSet resultSet = null;
        try {
            /**
             * 设置连接属性,使得可获取到表的REMARK(备注)
             */
            DatabaseMetaData dbmd = conn.getMetaData();
            String[] types = {"TABLE"};
            resultSet = dbmd.getTables(null, "null", "%", types);
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");  //表名
                String tableType = resultSet.getString("TABLE_TYPE");  //表类型
                String remarks = resultSet.getString("TABLE_REMARKS");       //表备注
                System.out.println(tableName + " - " + tableType + " - " + remarks);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }
    
    /**
     * describe tableName
     * or
     * desc tableName;
     *
     * @param cmd
     */
    private static void doDescribe(String cmd, String tableName) {
        ResultSet resultSet = null;
        try {
            String[] types = {"TABLE"};
            DatabaseMetaData dbmd = conn.getMetaData();
            resultSet = dbmd.getTables(tableName, null, "%", types);
            resultSet.beforeFirst();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int size = resultSetMetaData.getColumnCount();
            System.out.println("size = " + size);
            for (int i = 1; i <= size; i++) {
                String fieldName = resultSetMetaData.getColumnName(i);
                String type = resultSet.getString(fieldName);  //类型
                System.out.println("字段名称：" + fieldName + " Type:" + type);
            }
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void doQuery(String cmd, String cmdf) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            ResultSetMetaData md = rs.getMetaData();
            int numcols = md.getColumnCount();
            int totalwidth = 0;
            
            // print header
            for (int i = 1; i <= numcols; i++) {
                int width = md.getColumnDisplaySize(i);
                totalwidth += width;
                String fmt = "%" + width + "s";
                if (cmdf.startsWith("EXPLAIN"))
                    System.out.format("%s", md.getColumnName(i));
                else
                    System.out.format(fmt, md.getColumnName(i));
            }
            
            System.out.println();
            for (int i = 0; i < totalwidth; i++)
                System.out.print("-");
            if (!cmdf.startsWith("EXPLAIN"))
                System.out.println();
            
            rs.beforeFirst();
            // print records
            while (rs.next()) {
                for (int i = 1; i <= numcols; i++) {
                    String fldname = md.getColumnName(i);
                    int fldtype = md.getColumnType(i);
                    String fmt = "%" + md.getColumnDisplaySize(i);
                    if (fldtype == Types.INTEGER)
                        System.out.format(fmt + "d", rs.getInt(fldname));
                    else if (fldtype == Types.BIGINT)
                        System.out.format(fmt + "d", rs.getLong(fldname));
                    else if (fldtype == Types.DOUBLE)
                        System.out.format(fmt + "f", rs.getDouble(fldname));
                    else
                        System.out.format(fmt + "s", rs.getString(fldname));
                }
                System.out.println();
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void doUpdate(String cmd) {
        try {
            Statement stmt = conn.createStatement();
            int howmany = stmt.executeUpdate(cmd);
            System.out.println(howmany + " records processed");
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
