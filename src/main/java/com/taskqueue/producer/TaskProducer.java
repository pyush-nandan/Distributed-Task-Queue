package com.taskqueue.producer;

import com.taskqueue.db.ConnectionPool;

import java.sql.*;

public class TaskProducer {
    private static final String INSERT_TASK_SQL = "INSERT INTO tasks (payload) VALUES (?::jsonb)";

    public long enqueue(String jsonPayload) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_TASK_SQL, Statement.RETURN_GENERATED_KEYS)){
            ps.setString(1, jsonPayload);
            ps.executeUpdate();
            try(ResultSet rs = ps.getGeneratedKeys()){
                if(rs.next()){
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Failed to enqueue task, no ID obtained.");
    }
}
