package org.core;

import org.core.enums.LogStatus;
import org.core.enums.TaskName;
import org.core.model.Configuration;
import org.core.model.MailConfig;
import org.core.util.PropertiesHelper;

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
    String emailToSend;
    private String lotteryDate;

    public Controller(int configId) {
        this.configId = configId;
        dataCrawler = new DataCrawler();
        //load config to class
        PropertiesHelper.loadProperties();
        dbHelperCtl = new JDBCHelper(PropertiesHelper.JDBC_CTL, PropertiesHelper.JDBC_USERNAME, PropertiesHelper.JDBC_PASSWORD);
        dbHelperStaging = new JDBCHelper(PropertiesHelper.JDBC_STAGING, PropertiesHelper.JDBC_USERNAME, PropertiesHelper.JDBC_PASSWORD);
        dbHelperDataWarehouse = new JDBCHelper(PropertiesHelper.JDBC_DW, PropertiesHelper.JDBC_USERNAME, PropertiesHelper.JDBC_PASSWORD);
        logService = LogService.getLogService(dbHelperCtl);
        initConfig();
        emailToSend = PropertiesHelper.DEFAULT_EMAIL;
        if (config != null) {
            emailToSend = config.getEmailToSend();
        }
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
                this.config.setEmailToSend(rs.getString("email_to_send"));
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
    public void crawl() {
        dataCrawler.init(config.getBaseUrl(), this.destination);
        try {
            var message = "Extracting data from %s".formatted(config.getBaseUrl());
            System.out.println(message);
            dataCrawler.crawlTargetDate(this.lotteryDate);
            logService.insertLog(LogFactory.createExtractingDataLog(configId, message, TaskName.DATASOURCE_TO_FILE, 1));
            var successMessage = "Crawled data successfully, file saved at %s".formatted(destination);
            System.out.println(successMessage);
            System.out.println("Sending notification mail");
            mailService.sendEmail(PropertiesHelper.DEFAULT_EMAIL, "KQXS green process 1 - Data source to file", successMessage);
            logService.insertLog(LogFactory.createSuccessExtractLog(configId, successMessage, TaskName.DATASOURCE_TO_FILE, 1));
            logService.insertLog(LogFactory.createReadyFileLog(configId, successMessage, TaskName.DATASOURCE_TO_FILE, 1));
        } catch (IOException e) {
            String errorMessage = "Crawled data from %s failed";
            mailService.sendEmail(PropertiesHelper.DEFAULT_EMAIL, "KQXS green process 1 - Data source to file", errorMessage);
            logService.insertLog(LogFactory.createFailureExtractLog(configId, errorMessage, TaskName.DATASOURCE_TO_FILE, 1));
            throw new RuntimeException(e);
        }
    }


    // Phan nay se thuoc ve Ngoc Diep
    public void fileToStaging() {
        var currentStatus = logService.getCurrentStatus();
        if (currentStatus != (LogStatus.READY_FILE)) {
            System.out.println(currentStatus);
            crawl();
        }
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
            var mess = "Loading data from csv file to Staging successfully";
            System.out.println(mess);
            logService.insertLog(LogFactory.createSuccessStagingLog(configId, mess, TaskName.FILE_TO_STAGING, 1));
            logService.insertLog(LogFactory.createReadyWarehouseLog(configId, mess, TaskName.FILE_TO_STAGING, 1));
            mailService.sendEmail(emailToSend, "KQXs green process 2 - File to staging", mess);
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
        if (logService.getCurrentStatus() != LogStatus.READY_WAREHOUSE) {
            fileToStaging();
        }
        var checkingMessage = "Checking if staging is available";
        System.out.println(checkingMessage);
        var checkingSql = "{CALL IsStagingDataAvailable(?)}";
        // 1.1 Checking is staging database has data available
        // If available then continue otherwise stop and insert log
        var isAvailable = dbHelperDataWarehouse.executeProcedure(checkingSql);
        if (!isAvailable) {
            System.out.println("Staging is not available");
            logService.insertLog(LogFactory.createFailureWarehouseLog(configId, "Staging is not available", TaskName.STAGING_TO_DW, 1));
            return;
        }
        var readyWarehouseMessage = "Staging is available";
        System.out.println(readyWarehouseMessage);
        var checkDuplicatedMessage = "Checking if staging is loaded";
        System.out.println(checkDuplicatedMessage);
        //1.2 Checking if staging data is loaded to data warehouse database before
        // if loaded we just skip it
        var isLoaded = dbHelperDataWarehouse.executeProcedure("{CALL IsStagingDataLoaded(?)}");
        if (isLoaded) {
            System.out.println("Staging is loaded");
            return;
        }
        var loadingWarehouseMessage = "Calling procedure loading from staging to data warehouse";
        System.out.println(loadingWarehouseMessage);
        // 1.3 Calling stored procedure that loads data from staging database to data warehouse
        String sql = "{CALL LoadDataWarehouse()}";
        try {
            //1.4 Inserting a log notify that loading is undergone
            logService.insertLog(LogFactory.createLoadingWarehouseLog(configId, loadingWarehouseMessage, TaskName.STAGING_TO_DW, 1));
            dbHelperDataWarehouse.procedure(sql);
            // 1.5 if succeed we insert success log
            var successWarehouseMessage = "Loading from staging to data warehouse successfully";
            logService.insertLog(LogFactory.createSuccessWarehouseLog(configId, successWarehouseMessage, TaskName.STAGING_TO_DW, 1));
            logService.insertLog(LogFactory.createReadyDatamartLog(configId, successWarehouseMessage, TaskName.STAGING_TO_DW, 1));
            System.out.println(successWarehouseMessage);
            // 1.6 and send mail
            mailService.sendEmail(emailToSend, "KQSX green process 3 - Staging to DW result", successWarehouseMessage);
        } catch (SQLException e) {
            // if failed a mail will be sent and a log also will be inserted to log table
            mailService.sendEmail(emailToSend, "[Error]Staging to DW result", e.getMessage());
            logService.insertLog(LogFactory.createFailureWarehouseLog(configId, e.getMessage(), TaskName.STAGING_TO_DW, 1));
            throw new RuntimeException(e);
        }
    }

    // Phan nay Thuong se chiu trach nhiem
    public void dwToDM() {
        logService.insertLog(LogFactory.createReadyExtractLog(configId, "Datasource is ready to extract", TaskName.DW_TO_DM, 1));
    }

    public void auto(String lotteryDate) {
        try {
            this.setDate(lotteryDate);
            crawl();
            Thread.sleep(200);
            fileToStaging();
            Thread.sleep(200);
            stagingToDW();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    private void archive(int totalDays) {
        LocalDate currentDate = LocalDate.now().minusDays(1);
        for (int i = 0; i < totalDays; i++) {
            String formattedDate = currentDate.format(DATE_FOMATTER);
            auto(formattedDate);
            currentDate = currentDate.minusDays(1);
        }
    }

    public void setDate(String lotteryDate) {
        this.lotteryDate = lotteryDate;
    }
}
