package org.core.model;

import org.core.enums.Status;

public class MLog {
    private int id;
    private String message;
    private int idConfig;
    private Status status;
    private String taskName;

    public MLog() {

    }

    public MLog(int id, String message, int idConfig, Status status, String taskName) {
        this.id = id;
        this.message = message;
        this.idConfig = idConfig;
        this.status = status;
        this.taskName = taskName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getIdConfig() {
        return idConfig;
    }

    public void setIdConfig(int idConfig) {
        this.idConfig = idConfig;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
