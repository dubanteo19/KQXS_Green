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
    JDBCHelper dbHelperCtl, dbHelperStaging, dbHelperDataWarehouse, dbHelperDataMart;
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
        dbHelperDataMart = new JDBCHelper(Constant.JDBC_DM, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
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

    //1. Khởi tạo kết nối control.db
    //2. Kết nối đến controller.config
    public void fileToStaging() {
        //3. Kiểm tra hôm nay dữ liệu đã được crawl về chưa
        var currentStatus = logService.getCurrentStatus();
        if (currentStatus != (LogStatus.READY_FILE)) {
            System.out.println(currentStatus);
            crawl();
        }
        //4. Truncate bảng staging
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
            //5. Lấy đường dẫn file csv cần load
            //6. Kết nối đến staging db
            //7. Xây dựng câu truy vấn load dữ liệu
            //8. Thực thi câu truy vấn dữ liệu
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
            //9. Cập nhật trạng thái trong log
            logService.insertLog(LogFactory.createSuccessStagingLog(configId, mess, TaskName.FILE_TO_STAGING, 1));
            logService.insertLog(LogFactory.createReadyWarehouseLog(configId, mess, TaskName.FILE_TO_STAGING, 1));
            //10.Gửi mail thông báo
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
        // 1. Kiểm tra data warehouse có sẵn không
        var checkingMessage = "Checking if data warehouse is available";
        System.out.println(checkingMessage);
        var checkingSql = "{CALL IsDataWarehouseAvailable(?)}";
        var isAvailable = dbHelperDataWarehouse.executeProcedure(checkingSql);

        // 1.1 Nếu data warehouse không khả dụng thì dừng lại và ghi log
        if (!isAvailable) {
            System.out.println("Data warehouse is not available");
            logService.insertLog(LogFactory.createFailureWarehouseLog(configId, "Dữ liệu không có sẵn trong kho dữ liệu", TaskName.DW_TO_DM, 1));
            return;
        }

        // 1.2 Nếu data warehouse sẵn sàng, ghi log
        var readyMessage = "Data warehouse is available";
        System.out.println(readyMessage);
        logService.insertLog(LogFactory.createReadyWarehouseLog(configId, readyMessage, TaskName.DW_TO_DM, 1));

        // 2. Kiểm tra xem data mart đã được tải chưa
        var checkDuplicatedMessage = "Checking if data mart is loaded";
        System.out.println(checkDuplicatedMessage);
        var isLoaded = dbHelperDataWarehouse.executeProcedure("{CALL IsDataLoadedToDM(?)}");  // Thủ tục kiểm tra đã tải chưa

        // 2.1 Nếu dữ liệu đã được tải, thì không cần làm gì thêm
        if (isLoaded) {
            System.out.println("Data mart is loaded");
            logService.insertLog(LogFactory.createSuccessDatamartLog(configId, "Dữ liệu đã được tải lên kho dữ liệu mart", TaskName.DW_TO_DM, 1));
            return;
        }

        // 3. Gọi procedure để tải dữ liệu từ data warehouse lên data mart
        var loadingMessage = "Calling procedure loading from data warehouse to data mart";
        System.out.println(loadingMessage);
        // 3.1 Thêm log thông báo quá trình tải đang thực hiện
        logService.insertLog(LogFactory.createLoadingDatamartLog(configId, loadingMessage, TaskName.DW_TO_DM, 1));
        String sql = "{CALL LoadDataMart()}";
        try {
            dbHelperDataWarehouse.procedure(sql);

            // 3.2 Nếu thành công, ghi log success và gửi email thông báo
            var successMessage = "Loading from data warehouse to data mart successfully";
            System.out.println(successMessage);
            logService.insertLog(LogFactory.createSuccessDatamartLog(configId, successMessage, TaskName.DW_TO_DM, 1));
            mailService.sendEmail(Constant.DEFAULT_EMAIL, "Processing 4", successMessage);
        } catch (SQLException e) {
            // 3.3 Nếu gặp lỗi, ghi log lỗi và gửi email thông báo lỗi
            var errorMessage = "Error when loading from data warehouse to data mart" + e.getMessage();
            System.out.println(errorMessage);
            logService.insertLog(LogFactory.createFailureDatamartLog(configId, errorMessage, TaskName.DW_TO_DM, 1));
            throw new RuntimeException(e);
        }
    }


    public void auto(String lotteryDate) {
        try {
            this.setDate(lotteryDate);
            crawl();
            Thread.sleep(200);
            fileToStaging();
            Thread.sleep(200);
            stagingToDW();
            Thread.sleep(200);
            dwToDM();
       
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
