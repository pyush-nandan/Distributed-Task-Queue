package com.taskqueue.worker;

import com.taskqueue.db.ConnectionPool;
import com.taskqueue.model.Task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TaskExecutor implements Runnable{
    private final Task task;
    private final String queueName;

    public TaskExecutor(Task task, String queueName) {
        this.task = task;
        this.queueName = queueName;
    }

    @Override
    public void run(){
        try{
            //Simulate task processing
//            System.out.printf("Processing Task ID: %d, Payload: %s, Retry count: %d%n", task.getId(), task.getPayload(), task.getRetryCount());
            Thread.sleep(50); //Simulate time taken to complete the task
            markCompleted(task.getId());
        }
        catch (Exception e){
            handleFailure(task.getId(), task.getRetryCount(), e);
        }
    }

    public void markCompleted(long taskID){
        //Update task status to completed in DB
        String updateSQL = String.format("UPDATE %s SET status = 'COMPLETED', updated_at = NOW() WHERE id = ?", queueName);
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(updateSQL)){
            ps.setLong(1, taskID);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Failed to mark task as completed: " + e.getMessage());
        }
    }

    public void handleFailure(long taskID, int retryCount, Exception e){
        System.err.printf("Task %d failed (retry %d): %s%n", taskID, retryCount, e.getMessage());
        try (Connection conn = ConnectionPool.getConnection()){
            String updateSQL;
            if (retryCount < 3){
                updateSQL = String.format("UPDATE %s SET status = 'PENDING', retry_count = retry_count + 1, locked_at = NULL WHERE id = ?", queueName);
            } else {
                updateSQL = String.format("UPDATE %s SET status = 'FAILED', updated_at = NOW() WHERE id = ?", queueName);
            }
            try (PreparedStatement ps = conn.prepareStatement(updateSQL)){
                ps.setLong(1, taskID);
                ps.executeUpdate();
            } catch (SQLException ex) {
                System.err.println("Failed to update task status after failure: " + ex.getMessage());
            }
        }
        catch (SQLException ex){
            System.err.println("Failed to handle task failure: " + ex.getMessage());
        }
    }
}
