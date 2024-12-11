package org.core;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

public class DataCrawler {
    String baseUrl;
    String station;
    String region;
    String date;
    String url;
    String destination;
    String fileName;
    Map<String, List<String>> kqxs = new HashMap<>();

    public DataCrawler() {
        this.date = LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
    }

    public void addRow(List<String> kqxs) {
        try {
            FileWriter fw = new FileWriter(fileName, true);
            StringBuilder row = new StringBuilder();
            // Iterate over kqxs and append each value with a comma
            for (int i = 0; i < kqxs.size(); i++) {
                row.append(kqxs.get(i));
                if (i < kqxs.size() - 1) {
                    row.append(","); // Add comma between values, but not after the last one
                }
            }
            fw.write(row.toString());
            fw.write("\n");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            logError("Error writing row to CSV: " + e.getMessage());
        }
    }

    public void init(String baseUrl, String destination) {
        this.baseUrl = baseUrl;
        this.destination = destination;
        this.fileName = destination;
    }

    public void buildUrl() {
        if (baseUrl != null && !baseUrl.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(baseUrl);
            sb.append("/");
            sb.append("?date=");
            sb.append(date);
            this.url = sb.toString();
            log("URL built successfully: " + this.url);
        } else {
            logError("Base URL is not set or is empty!");
        }
    }

    public void crawlTargetDate(String date) throws IOException {
        writeHeader();
        LocalDateTime now = LocalDateTime.now();
        boolean isDataAvailable = isDataAvailable();
        if (!isDataAvailable) {
            logError("Data not available for the current date or time.");
        }
        now = now.minusDays(1);
        this.date = now.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        if (date != null) {
            this.date = date;
        }
        buildUrl();
        try {
            Document doc = Jsoup.connect(url).get();
            crawlRegion(doc, 2);
            crawlRegion(doc, 3);
        } catch (IOException e) {
            logError("Error during crawling: " + e.getMessage());
            throw e;
        }
    }

    private boolean isDataAvailable() {
        var now = LocalDateTime.now();
        if (now.getHour() == 16) {
            return now.getMinute() <= 30;
        }
        return now.getHour() > 16;
    }

    public void crawlRegion(Document doc, int regionCode) {
        kqxs.clear();
        List<String> stations = new ArrayList<>();
        String tableId = "result_" + regionCode;
        Element table = doc.getElementById(tableId);
        if (table == null) return;
        for (var e : table.getElementsByClass("wrap-text")) {
            stations.add(e.text());
        }
        Elements elements = table.getElementsByAttribute("data-prize");
        int count = 0;
        for (var station : stations) {
            List<String> temp = new ArrayList<>();
            temp.add(getRegionName(regionCode));
            temp.add(station);
            temp.add(date);
            kqxs.put(station, temp);
        }
        for (Element element : elements) {
            String value = element.attr("data-value");
            int numberOfColumns = regionCode == 2 ? 3 : 2;
            int stationIndex = count % numberOfColumns;
            kqxs.get(stations.get(stationIndex)).add(value);
            count++;
        }
        exportCSV(kqxs);
    }

    private String getRegionName(int regionCode) {
        if (regionCode == 1) {
            return "Miền Bắc";
        }
        if (regionCode == 2) {
            return "Miền Nam";
        }
        if (regionCode == 3) {
            return "Miền Trung";
        }
        return "Undefined";
    }

    public void exportCSV(Map<String, List<String>> kqxs) {
        for (String station : kqxs.keySet()) {
            addRow(kqxs.get(station));
        }
    }

    public void crawlAll(String destination) throws IOException {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        LocalDate startDate = getLocalDate();
        LocalDate currentDate = startDate;
        writeHeader();
        for (int i = 0; i < 365; i++) {
            this.date = currentDate.format(dateTimeFormatter);
            currentDate = currentDate.minusDays(1);
            buildUrl();
            try {
                Document doc = Jsoup.connect(url).get();
                Elements elements = doc.getElementsByAttribute("data-prize");
                if (elements.isEmpty()) continue;
                List<String> kqxs = new ArrayList<>();
                kqxs.add(this.region);
                kqxs.add(this.station);
                kqxs.add(this.date);
                int count = 1;
                for (Element element : elements) {
                    String value = element.attr("data-value");
                    kqxs.add(value);
                }
                addRow(kqxs);
            } catch (IOException e) {
                logError("Error during crawling for date " + this.date + ": " + e.getMessage());
            }
        }
    }

    private void writeHeader() {
        try {
            FileWriter fw = new FileWriter(fileName, false);
            fw.write("region,station,date,g1,g2,g3,g41,g42,g51,g52,g53,g54,g55,g56,g57,g6,g71,g72,g73,g8,g9\n");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            logError("Error writing header to CSV: " + e.getMessage());
        }
    }

    private LocalDate getLocalDate() {
        String[] split = this.date.split("-");
        return LocalDate.of(Integer.parseInt(split[2]), Integer.parseInt(split[1]), Integer.parseInt(split[0]));
    }

    public static void main(String[] args) {
        DataCrawler crawler = new DataCrawler();

        // Thiết lập base URL và đích đến cho crawl
        String destination = "E:/kqxs_daily.csv"; // Đổi đường dẫn file đến ổ E:
        crawler.init("https://your-lottery-website.com", destination);

        // Tạo URL cho ngày cụ thể hoặc sử dụng ngày hiện tại
        try {
            if (args.length > 0 && args[0].equals("-c")) {
                String configId = args[1]; // Ví dụ: "-c 1"
                // Xử lý cài đặt cấu hình từ `configId` ở đây
                // ...
            } else {
                crawler.crawlTargetDate(null); // Sử dụng ngày hiện tại
            }
        } catch (IOException e) {
            crawler.logError("Error during crawling: " + e.getMessage());
        }

        // Export kết quả ra file CSV vào ổ E:
        crawler.exportCSV(crawler.kqxs);
    }

    private String fromEmail = "21130211@st.hcmuaf.edu.vn";
    private String toEmail = "tbui35497@gmail.com";
    private String host = "smtp.example.com";
    private String port = "587";
    private String user = "your-username";
    private String password = "your-password";

    public void sendEmail(String subject, String body) {
        Properties properties = System.getProperties();
        properties.setProperty("mail.smtp.host", host);
        properties.setProperty("mail.smtp.port", port);
        properties.setProperty("mail.smtp.auth", "true");
        properties.setProperty("mail.smtp.starttls.enable", "true");

        Session session = Session.getInstance(properties, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, password);
            }
        });

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(fromEmail));
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);
            log("Sent message successfully....");
        } catch (MessagingException mex) {
            logError("Error while sending email: " + mex.getMessage());
        }
    }

    private void log(String message) {
        System.out.println("INFO: " + message);
    }

    private void logError(String message) {
        System.err.println("ERROR: " + message);
        try (FileWriter fw = new FileWriter("error_log.txt", true)) {
            fw.write(LocalDateTime.now() + ": " + message + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
