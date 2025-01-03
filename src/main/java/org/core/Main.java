package org.core;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args[0].equals("-help") || args[0].equals("-h")) {
            Controller.printHelp();
            return;
        }
        if (args.length < 3 || !args[1].equals("-c")) {
            System.out.println("Invalid usage. Specify a process and config ID with '-c'. Use -help for more information");
        }
        String process = args[0];
        int configId;
        String lotteryDate;
        try {
            configId = Integer.parseInt(args[2]);
        } catch (Exception e) {
            System.out.println("Invalid configuration ID. It must be a number");
            return;
        }
        try {
            lotteryDate = args[4];
        } catch (Exception e) {
            lotteryDate = null;
        }
        Controller controller = new Controller(configId);
        controller.setDate(lotteryDate);
        switch (process) {
            case "p1" -> controller.crawl();
            case "p2" -> controller.fileToStaging();
            case "p3" -> controller.stagingToDW();
            case "p4" -> controller.dwToDM();
            case "auto" -> controller.auto(lotteryDate);
        }
    }
}