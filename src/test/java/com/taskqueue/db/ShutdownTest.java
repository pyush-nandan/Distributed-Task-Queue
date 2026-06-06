package com.taskqueue.db;

import com.taskqueue.worker.WorkerNode;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShutdownTest extends BaseIntegrationTest{
    // This test will verify that the shutdown mechanism correctly signals the worker nodes to stop processing new tasks and allows them to finish their current tasks before exiting.

    @Test
    void testGracefulShutdownCompletesInFlightTasks() throws Exception{
        String insertSQL = "INSERT INTO tasks (payload) VALUES ('{\"test\":true}'::jsonb)";
        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement(insertSQL)){
            for(int i = 0; i < 3; i++){
                ps.executeUpdate();
            }
        }

        WorkerNode worker = new WorkerNode();
        worker.start();
        Thread.sleep(1000); //let tasks get claimed
        worker.shutdown();

        try(Connection conn = ConnectionPool.getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM tasks WHERE status = 'RUNNING'");
            ResultSet rs = ps.executeQuery()){
            rs.next();
            assertEquals(0, rs.getInt(1), "No task should be stuck in RUNNING after shutdown");
        }
    }
}
