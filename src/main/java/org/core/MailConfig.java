package org.core;

public class MailConfig {
    private int id;
    private String host;
    private int port;
    private String username;
    private String password;

    public MailConfig(int id, String host, int port, String username, String password) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public MailConfig() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
