package org.core.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class PropertiesHelper {
    public static String JDBC_CTL;
    public static String JDBC_STAGING;
    public static String JDBC_DW;
    public static String JDBC_USERNAME;
    public static String JDBC_PASSWORD;
    public static String DEFAULT_EMAIL;
    public static boolean enableMailService;

    public static void loadProperties() {
        Properties prop = new Properties();
        try {
            FileInputStream input = new FileInputStream("src/main/resources/config.properties");
            prop.load(input);
            DEFAULT_EMAIL = prop.getProperty("default_email");
            JDBC_USERNAME = prop.getProperty("username");
            JDBC_PASSWORD = prop.getProperty("password");
            enableMailService = prop.getProperty("enable_mail_service").equals("true");
            JDBC_DW = prop.getProperty("dw");
            JDBC_STAGING = prop.getProperty("staging");
            JDBC_CTL = prop.getProperty("ctl");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
