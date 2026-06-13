package com.taskqueue.worker;

import com.taskqueue.db.ConnectionPool;
import com.taskqueue.model.Task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WorkerNode {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ThreadPoolExecutor taskPool;
    private final String workerID;
    private final long pollInterval;
    private final String queueName;
    private final String pollSQL;
    private final String claimSQL;

    public WorkerNode(String workerID, int poolSize, long pollInterval, String queueName) {
        this.workerID = workerID;
        this.taskPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);
        this.pollInterval = pollInterval;
        this.queueName = queueName;
        this.pollSQL = String.format("""
            SELECT id, payload, retry_count FROM %s
            WHERE status = 'PENDING'
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """, queueName);

        this.claimSQL = String.format("""
            UPDATE %s
            SET status = 'RUNNING', locked_at = NOW()
            WHERE id = ?
            """, queueName);
    }

    public void start(){
        scheduler.scheduleAtFixedRate(this::poll, 0, pollInterval, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(new SweeperThread(queueName), 10, 10, TimeUnit.MINUTES);
    }

    public void poll(){
        try(Connection conn = ConnectionPool.getConnection()){
            conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(pollSQL);
                     ResultSet rs = ps.executeQuery()){
                    if(!rs.next()) {
                        conn.commit();
                        return;
                    }
                    Task task = new Task(rs.getLong("id"), rs.getString("payload"), rs.getInt("retry_count"));
                    try (PreparedStatement claim = conn.prepareStatement(claimSQL)){
                        claim.setLong(1, task.getId());
                        claim.executeUpdate();
                    }
                    conn.commit();
                    taskPool.submit(new TaskExecutor(task, queueName));
                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                }
        } catch (Exception e) {
            System.err.println("Error polling for tasks: " + e.getMessage());
        }
    }

    public void registerShutdownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            System.out.println("Shut down signal received. Shutting down Distributed Task Queue System...");
            scheduler.shutdown();
            taskPool.shutdown();
            try{
                if(!taskPool.awaitTermination(30, TimeUnit.SECONDS)){
                    System.err.println("Forcing shutdown - Tasks timed out.");
                    taskPool.shutdownNow();
                }
            } catch (InterruptedException e){
                taskPool.shutdownNow();
            }
            System.out.println("System shutdown complete.");
        }));
    }

    //method to allow integration testing
    public void shutdown() throws InterruptedException{
        scheduler.shutdown();
        taskPool.shutdown();
        taskPool.awaitTermination(30, TimeUnit.SECONDS);
    }
}
