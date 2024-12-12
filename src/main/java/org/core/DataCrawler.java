package org.core;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
            throw new RuntimeException(e);
        }
    }

    public void init(String baseUrl, String destination) {
        this.baseUrl = baseUrl;
        this.destination = destination;
        this.fileName = destination;
    }

    public void buildUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(baseUrl);
        sb.append("/");
        sb.append("?date=");
        sb.append(date);
        this.url = sb.toString();
        System.out.println("url built successfully %s".formatted(this.url));
    }

    public void crawlTargetDate(String date) throws IOException {
        writeHeader();
        LocalDateTime now = LocalDateTime.now();
        boolean isDataAvailable = isDataAvailable();
        if (!isDataAvailable) {
        }
        now = now.minusDays(1);
        this.date = now.format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        if (date != null) {
            this.date = date;
        }
        buildUrl();
        Document doc = Jsoup.connect(url).get();
        crawlRegion(doc, 2);
        crawlRegion(doc, 3);
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
        Collections.reverse(elements);
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
            Document doc = Jsoup.connect(url).get();
            Elements elements = doc.getElementsByAttribute("data-prize");
            if (elements.isEmpty()) continue;
            List<String> kqxs = new ArrayList<>();
            kqxs.add(this.region);
            kqxs.add(this.station);
            kqxs.add(this.date);
            List<String> temp = new ArrayList<>();
            for (Element element : elements) {
                System.out.println("Crawling with date");
                String value = element.attr("data-value");
                temp.add(value);
            }
            Collections.reverse(temp);
            System.out.println(temp);
            kqxs.addAll(temp);
            addRow(kqxs);
        }
    }

    private void writeHeader() {
        try {
            System.out.println("Writing header");
            FileWriter fw = new FileWriter(fileName, false);
            fw.write("region,station,lottery_date,g1,g2,g3,g41,g42,g51,g52,g53,g54,g55,g56,g57,g6,g71,g72,g73,g8,g9,\n");
            fw.flush();
            fw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private LocalDate getLocalDate() {
        String[] split = this.date.split("-");
        return LocalDate.of(Integer.parseInt(split[2]), Integer.parseInt(split[1]), Integer.parseInt(split[0]));
    }

}
