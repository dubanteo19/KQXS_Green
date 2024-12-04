package org.core;


import org.core.enums.LogLevel;
import org.core.enums.LogStatus;
import org.core.enums.TaskName;
import org.core.model.MLog;

public class LogFactory {

    // Helper method to map LogStatus to LogLevel
    private static LogLevel getLogLevelFromStatus(LogStatus status) {
        switch (status) {
            case SUCCESS_EXTRACT:
            case SUCCESS_STAGING:
            case SUCCESS_WAREHOUSE:
            case SUCCESS_DATAMART:
                return LogLevel.INFO;
            case FAILURE_EXTRACT:
            case FAILURE_STAGING:
            case FAILURE_WAREHOUSE:
            case FAILURE_DATAMART:
                return LogLevel.ERROR;
            case EXTRACTING_DATA:
            case LOADING_WAREHOUSE:
            case LOADING_DATAMART:
            case TRANSFORMING_WAREHOUSE:
            case TRANSFORMING_DATAMART:
                return LogLevel.DEBUG;
            case READY_EXTRACT:
            case READY_FILE:
            case READY_WAREHOUSE:
            case READY_DATAMART:
                return LogLevel.WARNING;
            case LOADING_STAGING:
                return LogLevel.DEBUG;
            default:
                return LogLevel.INFO;  // Default fallback
        }
    }

    // Factory method for each status

    public static MLog createReadyExtractLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.READY_EXTRACT, message, taskName, process);
    }

    public static MLog createExtractingDataLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.EXTRACTING_DATA, message, taskName, process);
    }

    public static MLog createSuccessExtractLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.SUCCESS_EXTRACT, message, taskName, process);
    }

    public static MLog createFailureExtractLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.FAILURE_EXTRACT, message, taskName, process);
    }

    public static MLog createReadyFileLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.READY_FILE, message, taskName, process);
    }

    public static MLog createLoadingStagingLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.LOADING_STAGING, message, taskName, process);
    }

    public static MLog createSuccessStagingLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.SUCCESS_STAGING, message, taskName, process);
    }

    public static MLog createFailureStagingLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.FAILURE_STAGING, message, taskName, process);
    }

    public static MLog createReadyWarehouseLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.READY_WAREHOUSE, message, taskName, process);
    }

    public static MLog createLoadingWarehouseLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.LOADING_WAREHOUSE, message, taskName, process);
    }

    public static MLog createTransformingWarehouseLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.TRANSFORMING_WAREHOUSE, message, taskName, process);
    }

    public static MLog createSuccessWarehouseLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.SUCCESS_WAREHOUSE, message, taskName, process);
    }

    public static MLog createFailureWarehouseLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.FAILURE_WAREHOUSE, message, taskName, process);
    }

    public static MLog createReadyDatamartLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.READY_DATAMART, message, taskName, process);
    }

    public static MLog createTransformingDatamartLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.TRANSFORMING_DATAMART, message, taskName, process);
    }

    public static MLog createLoadingDatamartLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.LOADING_DATAMART, message, taskName, process);
    }

    public static MLog createSuccessDatamartLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.SUCCESS_DATAMART, message, taskName, process);
    }

    public static MLog createFailureDatamartLog(int idConfig, String message, TaskName taskName, int process) {
        return createLog(idConfig, LogStatus.FAILURE_DATAMART, message, taskName, process);
    }

    // Helper method to create the log with status and level
    private static MLog createLog(int idConfig, LogStatus status, String message, TaskName taskName, int process) {
        LogLevel level = getLogLevelFromStatus(status);
        var log = new MLog();
        log.setIdConfig(idConfig);
        log.setLevel(level);
        log.setMessage(message);
        log.setStatus(status);
        log.setTaskName(taskName);
        log.setProcess(process);
        return log;
    }
}