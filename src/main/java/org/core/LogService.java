package org.core;

import org.core.enums.LogLevel;
import org.core.enums.LogStatus;
import org.core.model.MLog;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LogService {
    JDBCHelper dbHelperCtl;
    static LogService logService;

    private LogService(JDBCHelper dbHelperCtl) {
        this.dbHelperCtl = dbHelperCtl;
    }

    public static LogService getLogService(JDBCHelper dbHelperCtl) {
        if (logService == null) {
            return new LogService(dbHelperCtl);
        }
        return logService;
    }

    public LogStatus getCurrentStatus() {
        var sql =
                """
                        SELECT * FROM log ORDER BY create_time DESC LIMIT 1;
                        """;
        try {
            ResultSet resultSet = dbHelperCtl.executeQuery(sql);
            while (resultSet.next()) {
                LogStatus.valueOf(resultSet.getString("status"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
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
