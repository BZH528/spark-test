package com.bzh.bigdata.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MysqlUtils {

    private static final String DBDRIVER = "com.mysql.jdbc.Driver";
    private static final String DBURL = "jdbc:mysql://192.168.10.102:3306/highwayinfo";
    private static final String DBUSER = "hive";
    private static final String DBPASSWORD = "123456";
    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlUtils.class);

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(DBDRIVER);
            conn = DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static String readSQLfile(String path) {
        LOGGER.info("path: " + path);
        File file = new File(path);
        if (file ==  null) {
            LOGGER.info("file对象为空！");
        }
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        InputStreamReader is = new InputStreamReader(fileInputStream);
        BufferedReader reader = new BufferedReader(is);
        String line;
        StringBuilder builder = new StringBuilder();
        try {
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(" ");
            }
            reader.close();
            is.close();
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return String.valueOf(builder);
    }

    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(PreparedStatement pstmt) {
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void getHighWayInfoFromMysql(String sqlStr, List<String> longotude, List<String> latitude) {
        Connection connection = MysqlUtils.getConnection();
        String sql = sqlStr;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                longotude.add(resultSet.getString(1));
                latitude.add(resultSet.getString(2));
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    //测试
    /*public static void main(String[] args) {
        Connection connection = MysqlUtils.getConnection();
        try {
            Statement statement = connection.createStatement();
            String sql = "select * from baomao_g65 limit 100";
            ResultSet resultSet = statement.executeQuery(sql);
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    String value = resultSet.getString(i);
                    builder.append(value);
                    if (i != columnCount) {
                        builder.append(",");
                    }
                }
                System.out.println(builder.toString());
            }
            MysqlUtils.close(resultSet);
            MysqlUtils.close(statement);
            MysqlUtils.close(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }*/

    public static void main(String[] args) {
        List<String> logintitudes = new ArrayList<>();
        List<String> latitudes = new ArrayList<>();
        Properties properties = MyPropertiesUtil.load("config.properties");
        String sqlfile_path = properties.getProperty("data.highway.sqlfile");
        // 获取mysql库表中经纬度的集合
        MysqlUtils.getHighWayInfoFromMysql(sqlfile_path, logintitudes, latitudes);


        MysqlUtils.getHighWayInfoFromMysql(sqlfile_path, logintitudes, latitudes);

        for (String logintitude : logintitudes) {
            System.out.println(logintitude);
        }
    }

}
