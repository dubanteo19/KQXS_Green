package org.core;

import org.core.enums.LogLevel;
import org.core.enums.LogStatus;
import org.core.model.MLog;

import java.sql.SQLException;

public class LogService {
    JDBCHelper dbHelperCtl;
    static LogService logService;

    private LogService(JDBCHelper dbHelperCtl) {
        this.dbHelperCtl = dbHelperCtl;
    }

    public static LogService getLogService() {
        if (logService == null) {
            return new LogService(new JDBCHelper(Constant.JDBC_CTL, Constant.JDBC_USERNAME, Constant.JDBC_PASSWORD));
        }
        return logService;
    }

    public void insertLog(MLog log) {
        var sql = """
                    INSERT INTO log (id_config, log_level, message, status, task_name, process)
                    VALUES (?, ?, ?, ?, ?, ?);
                """;
        try {
            dbHelperCtl.executeUpdate(sql, log.getIdConfig(), log.getLevel(), log.getMessage(), log.getStatus(), log.getTaskName(), log.getProcess());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
