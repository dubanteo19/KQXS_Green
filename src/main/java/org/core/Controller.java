package org.core;

import org.core.enums.TaskName;
import org.core.model.Configuration;
import org.core.model.MailConfig;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Controller {
    private static final DateTimeFormatter DATE_FOMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    String destination;
    Configuration config;
    JDBCHelper dbHelperCtl, dbHelperStaging, dbHelperDataWarehouse;
    DataCrawler dataCrawler;
    MailService mailService;
    LogService logService;
    int configId;

    public Controller(int configId) {
        this.configId = configId;
        logService = LogService.getLogService();
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
        System.out.println("auto automatically run all the jobs ");
        System.out.println("-c to specify configuration id");
        System.out.println("-d to specify lottery date eg: 10-10-2025");
    }

    // Trang se lam phan nay
    public void crawl(String lotteryDate) {
        dataCrawler.init(config.getBaseUrl(), this.destination);
        try {
            var message = "Extracting data from %s".formatted(config.getBaseUrl());
            System.out.println(message);
            dataCrawler.crawlTargetDate(lotteryDate);
            logService.insertLog(LogFactory.createExtractingDataLog(configId, message, TaskName.DATASOURCE_TO_FILE, 1));
            var successMessage = "Crawled data successfully, file saved at %s".formatted(destination);
            System.out.println(successMessage);
            System.out.println("Sending notification mail");
            mailService.sendEmail(Constant.DEFAULT_EMAIL, "KQXS green process 1 - Data source to file", successMessage);
            logService.insertLog(LogFactory.createSuccessExtractLog(configId, successMessage, TaskName.DATASOURCE_TO_FILE, 1));
        } catch (IOException e) {
            String errorMessage = "Crawled data from %s failed";
            mailService.sendEmail(Constant.DEFAULT_EMAIL, "KQXS green process 1 - Data source to file", errorMessage);
            logService.insertLog(LogFactory.createFailureExtractLog(configId, errorMessage, TaskName.DATASOURCE_TO_FILE, 1));
            throw new RuntimeException(e);
        }
    }


    // Phan nay se thuoc ve Ngoc Diep
    public void fileToStaging() {
        truncateStaging();
        try {
            dbHelperStaging.callProcedure("SET GLOBAL local_infile=true;");
            String sql = """
                        LOAD DATA LOCAL INFILE ?
                        INTO TABLE stg_lottery_data
                        FIELDS TERMINATED BY ','
                        ENCLOSED BY '"'
                        LINES TERMINATED BY '\n'
                        IGNORE 1 LINES
                        (region, station, @var_date, g1, g2, g3, g41, g42, g51, g52, g53, g54, g55, g56, g57, g6, g71, g72, g73, g8, g9)  
                        SET lottery_date = STR_TO_DATE(@var_date, '%d-%m-%Y');
                    """;
            dbHelperStaging.executeUpdate(sql, destination);
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

    // This part belongs to dubanteo19
    //
    public void stagingToDW() {
        var checkingMessage = "Checking if staging is available";
        System.out.println(checkingMessage);
        var checkingSql = "{CALL IsStagingDataAvailable(?)}";
        var isAvailable = dbHelperStaging.executeProcedure(checkingSql);
        if (!isAvailable) {
            System.out.println("Staging is not available");
            return;
        }
        var readyWarehouseMessage = "Staging is available";
        System.out.println(readyWarehouseMessage);
        var checkDuplicatedMessage = "Checking if staging is loaded";
        System.out.println(checkDuplicatedMessage);
        var isLoaded = dbHelperStaging.executeProcedure("{CALL IsStagingDataLoaded(?)}");
        if (isLoaded) {
            System.out.println("Staging is loaded");
            return;
        }
        var loadingWarehouseMessage = "Calling procedure loading from staging to data warehouse";
        System.out.println(loadingWarehouseMessage);
        String sql = "{CALL LoadDataWarehouse()}";
        try {
            logService.insertLog(LogFactory.createLoadingWarehouseLog(configId, loadingWarehouseMessage, TaskName.STAGING_TO_DW, 1));
            dbHelperStaging.procedure(sql);
            var successWarehouseMessage = "Loading from staging to data warehouse successfully";
            logService.insertLog(LogFactory.createSuccessWarehouseLog(configId, successWarehouseMessage, TaskName.STAGING_TO_DW, 1));
            System.out.println(successWarehouseMessage);
            mailService.sendEmail(Constant.DEFAULT_EMAIL, "Processing 3", successWarehouseMessage);
        } catch (SQLException e) {
            logService.insertLog(LogFactory.createFailureWarehouseLog(configId, e.getMessage(), TaskName.STAGING_TO_DW, 1));
            throw new RuntimeException(e);
        }
    }

    // Phan nay Thuong se chiu trach nhiem
    public void dwToDM() {

    }

    public void auto(String lotteryDate) {
        try {
            crawl(lotteryDate);
            Thread.sleep(100);
            fileToStaging();
            Thread.sleep(100);
            stagingToDW();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        Controller controller = new Controller(1);
        controller.archive(365);
    }

    private void archive(int totalDays) {
        LocalDate currentDate = LocalDate.now().minusDays(1);
        for (int i = 0; i < totalDays; i++) {
            String formattedDate = currentDate.format(DATE_FOMATTER);
            auto(formattedDate);
            currentDate = currentDate.minusDays(1);
        }
    }
}
