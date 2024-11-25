package org.core;

import java.time.LocalDate;

public class Log {
    private int logId;
    private String fileId;
    private String logMessage;
    private LocalDate timeStart;
    private LocalDate timeEnd;

    public Log(int logId, String fileId, String logMessage, LocalDate timeStart, LocalDate timeEnd) {
        this.logId = logId;
        this.fileId = fileId;
        this.logMessage = logMessage;
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
    }

    public Log() {
    }

    public int getLogId() {
        return logId;
    }

    public void setLogId(int logId) {
        this.logId = logId;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    public LocalDate getTimeStart() {
        return timeStart;
    }

    public void setTimeStart(LocalDate timeStart) {
        this.timeStart = timeStart;
    }

    public LocalDate getTimeEnd() {
        return timeEnd;
    }

    public void setTimeEnd(LocalDate timeEnd) {
        this.timeEnd = timeEnd;
    }

    @Override
    public String toString() {
        return "Log{" +
                "logId=" + logId +
                ", fileId='" + fileId + '\'' +
                ", logMessage='" + logMessage + '\'' +
                ", timeStart=" + timeStart +
                ", timeEnd=" + timeEnd +
                '}';
    }
}
