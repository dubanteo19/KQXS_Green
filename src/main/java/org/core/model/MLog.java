package org.core.model;

import org.core.enums.LogLevel;
import org.core.enums.LogStatus;
import org.core.enums.TaskName;

public class MLog {
    private int id;
    private String message;
    private LogLevel level;
    private int idConfig;
    private LogStatus status;
    private TaskName taskName;
    private int process;

    public MLog() {


    }

    public MLog(int id, String message, LogLevel level, int idConfig, LogStatus status, TaskName taskName, int process) {
        this.id = id;
        this.message = message;
        this.level = level;
        this.idConfig = idConfig;
        this.status = status;
        this.taskName = taskName;
        this.process = process;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTaskName(TaskName taskName) {
        this.taskName = taskName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LogLevel getLevel() {
        return level;
    }

    public void setLevel(LogLevel level) {
        this.level = level;
    }

    public int getIdConfig() {
        return idConfig;
    }

    public void setIdConfig(int idConfig) {
        this.idConfig = idConfig;
    }

    public LogStatus getStatus() {
        return status;
    }

    public void setStatus(LogStatus status) {
        this.status = status;
    }


    public int getProcess() {
        return process;
    }

    public TaskName getTaskName() {
        return taskName;
    }

    public void setProcess(int process) {
        this.process = process;
    }
}
