package com.taskqueue.db;

import com.taskqueue.worker.SweeperThread;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SweeperTest extends BaseIntegrationTest{

    @AfterEach
    void deleteTasks(){
        String deleteSQL = "DELETE FROM tasks";
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(deleteSQL)){
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error deleting tasks: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    void testSweeperThreadFixesZombieTasks() throws SQLException{
        String insertSQL = "INSERT INTO tasks (payload, status, locked_at, retry_count) VALUES ('{\"test\":true}'::jsonb, 'RUNNING', NOW() - INTERVAL '11 minutes', 2) RETURNING id";
        long taskID;
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(insertSQL);
            ResultSet rs = ps.executeQuery()){
            rs.next();
            taskID = rs.getLong("id");
        }

        SweeperThread sweeper = new SweeperThread("tasks");
        sweeper.run();

        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT status, retry_count FROM tasks WHERE id = ?")){
            ps.setLong(1, taskID);
            try(ResultSet rs = ps.executeQuery()){
                rs.next();
                assertEquals("PENDING", rs.getString("status"));
                assertEquals(3, rs.getInt("retry_count"));
            }
        }
    }
}
