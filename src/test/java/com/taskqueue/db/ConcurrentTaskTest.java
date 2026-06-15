package com.taskqueue.db;

import com.taskqueue.worker.WorkerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConcurrentTaskTest extends BaseIntegrationTest {

    @BeforeEach
    void insertTasks(){
        String InsertSQL = """
                INSERT INTO tasks (payload) VALUES (?::jsonb)
                """;
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(InsertSQL)){
            for(int i = 0; i < 3; i++){
                ps.setString(1, String.format("{\"task_id\": %d, \"data\": \"Concurrent task data %d\"}", i+1, i+1));
                ps.executeUpdate();
            }
        }
        catch (SQLException e){
            throw new RuntimeException("Error inserting tasks: " + e.getMessage(), e);
        }
    }

    @AfterEach
    void deleteTasks(){
        String deleteSQL = "DELETE FROM tasks";
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(deleteSQL)){
            ps.executeUpdate();
        }
        catch (SQLException e){
            throw new RuntimeException("Error deleting tasks: " + e.getMessage(), e);
        }
    }

    @Test
    void testConcurrentPolling(){
        String workerID = System.getenv().getOrDefault("WORKER_ID", "worker");
        WorkerNode worker = new WorkerNode(workerID, 5, 2000, "tasks");
        CyclicBarrier barrier = new CyclicBarrier(3);

        Runnable taskPolling = () -> {
            try{
                barrier.await();
                worker.poll();
            } catch (Exception e){
                throw new RuntimeException("Error in worker thread: " + e.getMessage(), e);
            }
        };

        Thread w1 = new Thread(taskPolling);
        Thread w2 = new Thread(taskPolling);
        Thread w3 = new Thread(taskPolling);

        w1.start(); w2.start(); w3.start();
        try{
            w1.join(); w2.join(); w3.join();
        } catch (InterruptedException e){
            System.err.println("Error waiting for threads to finish: " + e.getMessage());
        }

        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM tasks WHERE status = 'PENDING'")){
            ResultSet rs = ps.executeQuery();
            if(rs.next()){
                int pendingCount = rs.getInt(1);
                assertEquals(0, pendingCount, "All tasks should be picked up");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error verifying task status: " + e.getMessage(), e);
        }
    }
}
