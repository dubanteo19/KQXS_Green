package org.core;

public class Configuration {
    private String id;
    private String username;
    private String password;
    private String baseUrl;
    private String file_path;
    private String file_name;
    private String emailToSend;

    public Configuration(String id, String username, String password, String baseUrl, String file_path, String file_name, String emailToSend) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.baseUrl = baseUrl;
        this.file_path = file_path;
        this.file_name = file_name;
        this.emailToSend = emailToSend;
    }

    public Configuration() {

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getFile_path() {
        return file_path;
    }

    public void setFile_path(String file_path) {
        this.file_path = file_path;
    }

    public String getFile_name() {
        return file_name;
    }

    public void setFile_name(String file_name) {
        this.file_name = file_name;
    }

    public String getEmailToSend() {
        return emailToSend;
    }

    public void setEmailToSend(String emailToSend) {
        this.emailToSend = emailToSend;
    }
}
