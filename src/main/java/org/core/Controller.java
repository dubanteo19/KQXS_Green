package org.core;

import org.core.model.Configuration;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Controller {
    String baseUrl;
    String destination;
    Configuration config;
    JDBCHelper dbHelperCtl, dbHelperStaging, dbHelperDataWarehouse;
    DataCrawler dataCrawler;
    MailService mailService;
    int configId;

    public Controller(int configId) {
        this.configId = configId;
        dataCrawler = new DataCrawler();
        dbHelperCtl = new JDBCHelper(Constant.JDBC_CTL, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
        dbHelperStaging = new JDBCHelper(Constant.JDBC_STAGING, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
        dbHelperDataWarehouse = new JDBCHelper(Constant.JDBC_DW, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
        initConfig();
        initMailConfig();
    }

    private void initMailConfig() {
        try {
            ResultSet rs = dbHelperCtl.executeQuery("select * from mail_config");
            MailConfig mailConfig = null;
            while (rs.next()) {
                mailConfig = new MailConfig();
                mailConfig.setId(rs.getInt("id"));
                mailConfig.setPort(rs.getInt("port"));
                mailConfig.setHost(rs.getString("host"));
                mailConfig.setPassword(rs.getString("password"));
                mailConfig.setUsername(rs.getString("username"));
            }
            this.mailService = new MailService(mailConfig);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void initConfig() {
        try {
            ResultSet rs = dbHelperCtl.executeQuery("Select * from config where id =?", configId);
            while (rs.next()) {
                this.config = new Configuration();
                this.config.setId(String.valueOf(rs.getInt("id")));
                this.config.setBaseUrl(rs.getString("base_url"));
                this.config.setFile_name(rs.getString("file_name"));
                this.config.setFile_path(rs.getString("file_path"));
            }
            this.destination = config.getFile_path() + File.separator + config.getFile_name();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printHelp() {
        System.out.println("p1 crawl data from data source to file");
        System.out.println("p2 load file csv to staging database");
        System.out.println("p3 staging to data warehouse");
        System.out.println("p4 data warehouse to data mart");
        System.out.println("-c to specify configuration id");
    }

    public void crawl() {
        dataCrawler.init(config.getBaseUrl(), this.destination);
        try {
            dataCrawler.crawlDaily();
            System.out.println("Crawled data successfully, file saved at " + destination);
            System.out.println("Sending notification mail");
            String body = "Crawled data form data source and save to csv file successfully";
            mailService.sendEmail(Constant.DEFAULT_EMAIL, "KQXS green process 1", body);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void fileToStaging() {
        truncateStaging();
        String sql = " LOAD DATA LOCAL INFILE '" + destination + "' " +
                "INTO TABLE stg_lottery_data " +
                "FIELDS TERMINATED BY ',' " +
                "ENCLOSED BY '\"' " +
                "LINES TERMINATED BY '\\n' " +
                "IGNORE 1 LINES " +
                "(region, station, @var_date, g1, g2, g3, g41, g42, g51, g52, g53, g54, g55, g56, g57, g6, g71, g72, g73, g8, g9) " +
                "SET lottery_date = STR_TO_DATE(@var_date, '%d-%m-%Y');";

        try {
            System.out.println("Loading csv to staging....");
            dbHelperStaging.callProcedure(sql);
            System.out.println("Loading successfully");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void truncateStaging() {
        System.out.println("Truncating staging database...");
        String sql = "TRUNCATE TABLE stg_lottery_data";
        try {
            dbHelperStaging.callProcedure(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void stagingToDW() {
        System.out.println("Calling procedure loading from stating to data warehouse");
        String sql = "{CALL LoadDataWarehouse()}";
        try {
            dbHelperStaging.procedure(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
