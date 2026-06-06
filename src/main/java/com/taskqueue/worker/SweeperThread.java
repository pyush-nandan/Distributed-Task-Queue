package com.taskqueue.worker;

import com.taskqueue.db.ConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SweeperThread implements Runnable{
    private static final String SWEEPER_ZOMBIE_SQL = """
            UPDATE tasks
            SET status = 'PENDING',locked_at = NULL, updated_at = NOW(), retry_count = retry_count + 1
            WHERE status = 'RUNNING' AND locked_at < NOW() - INTERVAL '10 minutes' AND retry_count < 3
            """;

    private static final String SWEEPER_FAIL_SQL = """
            UPDATE tasks
            SET status = 'FAILED', locked_at = NULL, updated_at = NOW()
            WHERE status = 'RUNNING' AND locked_at < NOW() - INTERVAL '10 minutes' AND retry_count >= 3
            """;

    @Override
    public void run(){
        try (Connection conn = ConnectionPool.getConnection();
             PreparedStatement pendingPs = conn.prepareStatement(SWEEPER_ZOMBIE_SQL);
             PreparedStatement failPs = conn.prepareStatement(SWEEPER_FAIL_SQL)){
            conn.setAutoCommit(false);
            try{
                int failed = failPs.executeUpdate();
                int rescued = pendingPs.executeUpdate();
                conn.commit();
                if (failed > 0){
                    System.out.printf("Sweeper thread: Marked %d stuck tasks as failed.%n", failed);
                }
                if (rescued > 0){
                    System.out.printf("Sweeper thread: Rescued %d stuck tasks.%n", rescued);
                }
            }
            catch (SQLException e){
                conn.rollback();
                throw e;
            }
        }
        catch (SQLException e){
            System.out.println("Sweeper thread stopped working: " + e.getMessage());
        }
    }
}
