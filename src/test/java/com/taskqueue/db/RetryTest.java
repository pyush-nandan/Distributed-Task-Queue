package com.taskqueue.db;

import com.taskqueue.model.Task;
import com.taskqueue.worker.TaskExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetryTest extends BaseIntegrationTest{
    // This test will verify that the retry mechanism correctly increments the retry count and updates the task status.
    // It will simulate a task failure and check if the retry count is updated in the database.

    @AfterEach
    void deleteTasks(){
        String deleteSQL = "DELETE FROM tasks";
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(deleteSQL)){
            ps.executeUpdate();
        }
        catch (SQLException e){
            System.err.println("Error deleting tasks: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    void testFailedTaskIsRequeued() throws SQLException{
        long taskID;
        String insertSQL = "INSERT INTO tasks (payload, retry_count) VALUES ('{\"test\":true}'::jsonb, 2) RETURNING id";
         try(Connection conn = ConnectionPool.getConnection();
             PreparedStatement ps = conn.prepareStatement(insertSQL);
             ResultSet rs = ps.executeQuery()){
             rs.next();
             taskID = rs.getLong("id");
         }
         Task task = new Task(taskID, "{\"test\":true}", 2);
         TaskExecutor executor = new TaskExecutor(task, "tasks");
         executor.handleFailure(taskID, 2, new Exception("Simulated failure"));

         try(Connection conn = ConnectionPool.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, retry_count FROM tasks WHERE id = ?")){
             ps.setLong(1, taskID);
             try(ResultSet rs = ps.executeQuery()){
                 rs.next();
                 String status = rs.getString("status");
                 int retryCount = rs.getInt("retry_count");
                 assertEquals("PENDING", status);
                 assertEquals(3, retryCount);
             }
         }
    }

    @Test
    void testExhaustedTaskIsMarkedFailed() throws SQLException{
        long taskID;
        String insertSQL = "INSERT INTO tasks (payload, retry_count) VALUES ('{\"test\":true}'::jsonb, 2) RETURNING id";
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(insertSQL)){
            ResultSet rs = ps.executeQuery();
            rs.next();
            taskID = rs.getLong("id");
        }

        Task task = new Task(taskID, "{\"test\":true}", 3);
        TaskExecutor executor = new TaskExecutor(task, "tasks");
        executor.handleFailure(taskID, 3, new Exception("Simulated failure"));

        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT status FROM tasks WHERE id = ?")){
            ps.setLong(1, taskID);
            ResultSet rs = ps.executeQuery();
            rs.next();
            String status = rs.getString("status");
            assertEquals("FAILED", status);
        }
    }
}