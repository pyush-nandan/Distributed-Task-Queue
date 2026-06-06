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
    private final ThreadPoolExecutor taskPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);

    private static final String POLL_SQL = """
            SELECT id, payload, retry_count FROM tasks
            WHERE status = 'PENDING'
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private static final String CLAIM_SQL = """
            UPDATE tasks SET status = 'RUNNING', locked_at = NOW()
            WHERE id = ?""";

    public void start(){
        scheduler.scheduleAtFixedRate(this::poll, 0, 2, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new SweeperThread(), 10, 10, TimeUnit.MINUTES);
    }

    public void poll(){
        try(Connection conn = ConnectionPool.getConnection()){
            conn.setAutoCommit(false);
                try (PreparedStatement ps = conn.prepareStatement(POLL_SQL);
                     ResultSet rs = ps.executeQuery()){
                    if(!rs.next()) {
                        conn.commit();
                        return;
                    }
                    Task task = new Task(rs.getLong("id"), rs.getString("payload"), rs.getInt("retry_count"));
                    try (PreparedStatement claim = conn.prepareStatement(CLAIM_SQL)){
                        claim.setLong(1, task.getId());
                        claim.executeUpdate();
                    }
                    conn.commit();
                    taskPool.submit(new TaskExecutor(task));
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
