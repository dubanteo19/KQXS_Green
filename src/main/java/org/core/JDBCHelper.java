package org.core;

import java.sql.*;

public class JDBCHelper {
    private String jdbcURL;
    private String jdbcUsername;
    private String jdbcPassword;
    private Connection connection;

    // Constructor
    public JDBCHelper(String jdbcURL, String jdbcUsername, String jdbcPassword) {
        this.jdbcURL = jdbcURL;
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
    }

    // Method to establish connection
    public Connection connect() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(jdbcURL, jdbcUsername, jdbcPassword);
        }
        return connection;
    }

    // Method to close the connection
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Method to execute SELECT queries
    public ResultSet executeQuery(String query, Object... params) throws SQLException {
        PreparedStatement preparedStatement = prepareStatement(query, params);
        return preparedStatement.executeQuery();
    }

    public void callProcedure(String sql) throws SQLException {
        Statement statement = connect().createStatement();
        statement.execute(sql);
    }

    // Method to execute INSERT, UPDATE, DELETE queries
    public int executeUpdate(String query, Object... params) throws SQLException {
        PreparedStatement preparedStatement = prepareStatement(query, params);
        return preparedStatement.executeUpdate();
    }

    // Helper method to prepare a statement with parameters
    private PreparedStatement prepareStatement(String query, Object... params) throws SQLException {
        PreparedStatement preparedStatement = connect().prepareStatement(query);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setObject(i + 1, params[i]);
        }
        return preparedStatement;
    }

    // Method to close ResultSet and PreparedStatement
    public void closeResources(AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}