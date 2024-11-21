package org.core;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Controller {
    String baseUrl;
    String destination;
    Configuration config;
    JDBCHelper dbHelperCtl, dbHelperStaging, getDbHelperDataWarehouse;
    DataCrawler dataCrawler;
    int configId;

    public Controller(int configId) {
        this.configId = configId;
        dataCrawler = new DataCrawler();
        dbHelperCtl = new JDBCHelper(Constant.JDBC_CTL, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
        dbHelperStaging = new JDBCHelper(Constant.JDBC_STAGING, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
        initConfig();
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
            System.out.println("crawled data successfully, file saved at " + destination);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void fileToStaging() {
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
}
