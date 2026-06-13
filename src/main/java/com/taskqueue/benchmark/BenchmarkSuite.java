package com.taskqueue.benchmark;

import com.taskqueue.db.ConnectionPool;
import com.taskqueue.worker.WorkerNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.cdimascio.dotenv.Dotenv;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkSuite {
   private static final int TOTAL_TASKS = 10000;
   private static final int[] CLUSTER_SIZES = {1, 2, 4, 8};
   private static final int THREADS_PER_NODE = 5;

   record BenchmarkResult(int nodes, int totalThreads, double tps, double latencyMs){};

    public static void main(String[] args) throws Exception{
        System.out.println("=================================================");
        System.out.println("  Distributed Queue: Horizontal Scaling Profile  ");
        System.out.println("=================================================\n");

        setupExpandedConnectionPool(50);
        List<BenchmarkResult> results = new ArrayList<>();
        for(int nodeCount : CLUSTER_SIZES){
            results.add(runBenchmark(nodeCount));
        }
        printReport(results);
        System.exit(0);
    }

   private static BenchmarkResult runBenchmark(int nodeCount) throws Exception{
       int totalThreads = nodeCount * THREADS_PER_NODE;
       System.out.printf(">>> Deploying Cluster: %d Node(s) | %d Total Executors...%n", nodeCount, totalThreads);

       try(Connection conn = ConnectionPool.getConnection();
           Statement stmt = conn.createStatement()){
           stmt.execute("TRUNCATE TABLE benchmark_tasks RESTART IDENTITY");
       }

       generateLoad();
       List<WorkerNode> activeWorkers = new ArrayList<>();
       for (int i = 1; i <= nodeCount; i++){
           WorkerNode node = new WorkerNode("worker-" + i, THREADS_PER_NODE, 100, "benchmark_tasks");
           node.start();
           activeWorkers.add(node);
       }

       waitForCompletion();
       BenchmarkResult result = extractTelemetry(nodeCount, totalThreads);

       for(WorkerNode node : activeWorkers){
           node.shutdown();
       }
       System.out.printf("Cluster Shoutdown. Result: %.2f TPS | %.2f ms avg latency%n%n",
               result.tps(), result.latencyMs());

       return result;
   }

   private static void generateLoad() throws InterruptedException{
       System.out.println("Seeding queue with " + TOTAL_TASKS + " tasks...");
       ExecutorService executor = Executors.newFixedThreadPool(4);
       AtomicInteger tasksInserted = new AtomicInteger(0);
       String insertSQL = "INSERT INTO benchmark_tasks (payload, status) VALUES ('{\"benchmark\":true}'::jsonb, 'PENDING')";

       for(int i = 0; i < 4; i++){
           executor.submit(() -> {
               try(Connection conn = ConnectionPool.getConnection();
                   PreparedStatement ps = conn.prepareStatement(insertSQL)){
                   for(int j = 1; j <= TOTAL_TASKS / 4; j++){
                       ps.addBatch();
                       if(j % 500 == 0){
                           ps.executeBatch();
                           tasksInserted.addAndGet(500);
                       }
                   }
               } catch (Exception e){
                   System.err.println("Producer failed: " + e.getMessage());
               }
           });
       }
       executor.shutdown();
       executor.awaitTermination(5, TimeUnit.MINUTES);
       System.out.println("Done.");
   }

   private static void waitForCompletion() throws Exception {
       String countSQL = "SELECT COUNT(*) FROM benchmark_tasks WHERE status in ('PENDING', 'RUNNING')";
       while(true){
           Thread.sleep(1000);
           try(Connection conn = ConnectionPool.getConnection();
               PreparedStatement ps = conn.prepareStatement(countSQL);
               ResultSet rs = ps.executeQuery()){
               if(rs.next()){
                   int remaining = rs.getInt(1);
                   if(remaining == 0) break;
                   System.out.printf("\rProcessing... %d tasks remaining", remaining);
               }
           }
       }
       System.out.println("\nProcessing... 0 tasks remaining");
   }

   private static BenchmarkResult extractTelemetry(int nodes, int totalThreads) throws Exception {
       String sql = """
               WITH bounds AS(
                   SELECT MIN(created_at) AS start_time, MAX(updated_at) AS end_time, COUNT(*) as total_tasks
                   FROM benchmark_tasks WHERE status = 'COMPLETED'),
               latencies AS(
                   SELECT AVG(EXTRACT(EPOCH FROM (updated_at - locked_at)) * 1000) as avg_exec_time
                   FROM benchmark_tasks WHERE status = 'COMPLETED')
               SELECT
                   ROUND((bounds.total_tasks / EXTRACT(EPOCH FROM (bounds.end_time - bounds.start_time)))::numeric, 2) AS tps,
                   ROUND(latencies.avg_exec_time::numeric, 2) AS exec_latency
               FROM bounds, latencies
               """;

       try(Connection conn = ConnectionPool.getConnection();
           PreparedStatement ps = conn.prepareStatement(sql);
           ResultSet rs = ps.executeQuery()){
           if(rs.next()){
               return new BenchmarkResult(nodes, totalThreads, rs.getDouble("tps"), rs.getDouble("exec_latency"));
           }
       }
       return new BenchmarkResult(nodes, totalThreads, 0, 0);
   }

   private static void printReport(List<BenchmarkResult> results){
       System.out.println("=====================================================================");
       System.out.println("| Cluster Nodes | Total Executors | Throughput (TPS) | Avg Exec. Ms |");
       System.out.println("|---------------|-----------------|------------------|--------------|");
       for(BenchmarkResult r : results){
           System.out.printf("| %-13d | %-15d | %-16.2f | %-12.2f |%n", r.nodes(), r.totalThreads(), r.tps(), r.latencyMs());
       }
       System.out.println("=====================================================================");
   }

   private static void setupExpandedConnectionPool(int maxPoolSize){
       System.out.println("Configuring connection pool for local Docker benchmark...");
       HikariConfig config = new HikariConfig();

       config.setJdbcUrl("jdbc:postgresql://localhost:5432/task_queue_db");
       config.setUsername("benchmark_user");
       config.setPassword("benchmark_password");

       config.setMaximumPoolSize(maxPoolSize);
       ConnectionPool.overrideDataSource(new HikariDataSource(config));
   }

}
