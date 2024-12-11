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

    public void procedure(String sql) throws SQLException {
        connect();
        try {
            CallableStatement callableStatement = connect().prepareCall(sql);
            callableStatement.execute();
        } catch (SQLException ex) {
            System.out.println("Error: " + ex.getMessage());
            ex.printStackTrace();
        }
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
            Object param = params[i];
            if (param instanceof Enum<?>) {
                preparedStatement.setString(i + 1, ((Enum<?>) param).name());
            } else if (param instanceof String) {
                preparedStatement.setString(i + 1, (String) param);
            } else if (param instanceof Integer) {
                preparedStatement.setInt(i + 1, (Integer) param);
            } else if (param instanceof Double) {
                preparedStatement.setDouble(i + 1, (Double) param);
            } else if (param instanceof Boolean) {
                preparedStatement.setBoolean(i + 1, (Boolean) param);
            } else if (param == null) {
                preparedStatement.setNull(i + 1, Types.NULL);
            } else {
                preparedStatement.setObject(i + 1, param);
            }
        }
        return preparedStatement;
    }


    public boolean executeProcedure(String checkingSql) {
        try {
            CallableStatement callableStatement = connect().prepareCall(checkingSql);
            callableStatement.registerOutParameter(1, Types.BOOLEAN);
            // Execute the stored procedure
            callableStatement.executeUpdate();
            // Retrieve the OUT parameter value
            return callableStatement.getBoolean(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}