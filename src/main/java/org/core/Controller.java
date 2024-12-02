package org.core;

import java.io.File;
import java.io.IOException;
import java.sql.*;

public class Controller {
    String baseUrl;
    String destination;
    Configuration config;
    JDBCHelper dbHelperCtl, dbHelperStaging, getDbHelperDataWarehouse;
    DataCrawler dataCrawler;
    EmailService emailService = new EmailService();
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
        try {
//            1,2. Kết nối control.db ở trên
//            3. Kiểm tra log xem dữ liệu của ngày hôm nay đã được crawl về file chưa
            ResultSet logCheck = dbHelperCtl.executeQuery(
                    "SELECT log_message FROM controller.logs " +
                            "WHERE log_message = 'Load dữ liệu vào file csv' " +
                            " AND status = 'Success'" +
                            "AND file_id = ? " +
                            "AND DATE(timeStart) = CURDATE() " +
                            "ORDER BY timeStart DESC LIMIT 1",
                    config.getId());

            if (!logCheck.next()) {
//            3.1. Insert log nếu không tìm thấy log "Load dữ liệu về file" trong ngày hôm nay
                dbHelperCtl.executeUpdate(
                        "INSERT INTO controller.logs (file_id, status, log_message, timeStart) " +
                                "VALUES (?, 'Failed', 'Không tìm thấy log dữ liệu được crawl hôm nay', NOW())",
                        config.getId());
                System.out.println("Không tìm thấy log dữ liệu được crawl hôm nay. Dừng tiến trình.");
                return;
            }
//            5. Kết nối staging
//            6.  Ghi log trạng thái "Running"
            int logId = dbHelperCtl.executeUpdateReturnGeneratedKeys(
                    "INSERT INTO controller.logs (file_id, status, log_message, timeStart) " +
                            "VALUES (?, 'Running', 'Load file to staging', NOW())",
                    config.getId());

            try {
                // Chuẩn bị câu lệnh SQL cho việc gọi stored procedure
                String loadProcedureCall = "{CALL load_data_to_temp_table(?)}"; // Cả input parameter

                // Lấy ngày hôm nay (format yyyy-MM-dd)
                String todayDate = java.time.LocalDate.now().toString();
                Connection conn = DriverManager.getConnection(Constant.JDBC_STAGING, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD);
                try {
                    conn.setAutoCommit(false); // Tắt auto-commit
//                    7,8. Gọi procedure load dữ liệu từ file vào bảng tạm
                    // Gọi procedure load_data_to_temp_table và lấy câu lệnh SQL động
                    CallableStatement loadStmt = conn.prepareCall(loadProcedureCall);
                    loadStmt.setDate(1, java.sql.Date.valueOf(todayDate));

                    // Execute and capture the result of SELECT statement
                    ResultSet resultSet = loadStmt.executeQuery();
                    String loadSql = null;
                    if (resultSet.next()) {
                        loadSql = resultSet.getString("dynamic_sql");
                        System.out.println("Debug SQL: " + loadSql);
                    }

                    // Thực thi câu lệnh SQL động
                    Statement stmt = conn.createStatement();
                    stmt.execute(loadSql);
                    System.out.println("SQL executed successfully.");
//                    9. Insert dữ liệu từ bảng tạm vào staging
                    // Gọi procedure move_data_to_staging_table
                    String moveProcedureCall = "{CALL move_data_to_staging_table(?)}";
                    CallableStatement stmt2 = conn.prepareCall(moveProcedureCall);
                    stmt2.setDate(1, java.sql.Date.valueOf(todayDate));
                    stmt2.execute();
                    System.out.println("Stored procedure move_data_to_staging_table executed successfully.");

                    conn.commit(); // Commit transaction
                } catch (SQLException e) {
                    conn.rollback();
                    throw new RuntimeException("Transaction failed. Rolled back.", e);
                }

                // Kiểm tra dữ liệu trong bảng staging
                ResultSet rs = dbHelperStaging.executeQuery("SELECT COUNT(*) AS rowCount FROM stg_lottery_data");
                if (rs.next()) {
                    int rowCount = rs.getInt("rowCount");
                    System.out.println("Rows loaded into staging table: " + rowCount);
                } else {
                    System.out.println("No data found in staging table for date: " + todayDate);
                }

            } catch (SQLException e) {
                System.err.println("SQL Exception occurred: " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Error while executing stored procedure.", e);
            }

            //10.  Cập nhật log trạng thái "Success"
            dbHelperCtl.executeUpdate(
                    "UPDATE controller.logs SET status = 'Success', timeEnd = NOW() WHERE log_id = ?",
                    logId);

            System.out.println("Dữ liệu đã được load thành công vào staging.");
//        11. Gửi mail...
            String recipient = "21130315@st.hcmuaf.edu.vn";
            String subject = "Thông báo: Quy trình load dữ liệu vào staging hoàn tất";
            String body = "Chào bạn,\n\nQuy trình load dữ liệu từ file CSV vào staging ngày hôm nay đã hoàn tất thành công.\n\nTrân trọng,\nHệ thống";
            emailService.sendEmail(recipient, subject, body);
        } catch (SQLException e) {
            try {
                // Xử lý lỗi và ghi log Failed"
                dbHelperCtl.executeUpdate(
                        "UPDATE controller.logs SET status = 'Failed', log_message = ?, timeEnd = NOW() WHERE log_message = 'Load file to staging'",
                        e.getMessage());
            } catch (SQLException innerEx) {
                throw new RuntimeException("Lỗi khi ghi log trạng thái thất bại.", innerEx);
            }
            throw new RuntimeException("Lỗi khi chạy quy trình load file vào staging.", e);
        }


    }



}
