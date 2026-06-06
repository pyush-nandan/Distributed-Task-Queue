package com.taskqueue.monitor;

import com.sun.net.httpserver.HttpServer;
import com.taskqueue.db.ConnectionPool;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class ObservabilityServer {
    private final int port;
    private HttpServer server;

    public ObservabilityServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/status", exchange -> {
            try{
                String json = buildJson();
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try(OutputStream os = exchange.getResponseBody()){
                    os.write(bytes);
                }
            } catch (SQLException e){
                String error = "{\"error\": \"DB failure\"}";
                byte[] bytes = error.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, bytes.length);
                try(OutputStream os = exchange.getResponseBody()){
                    os.write(bytes);
            }
        }});
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.start();
        System.out.println("Observability server started on port " + port);
    }

    private String buildJson() throws SQLException {
        String metricsQuery = """
                SELECT status, count(*) as task_count
                FROM tasks
                GROUP BY status
                ORDER BY task_count DESC
                """;
        try (Connection conn = ConnectionPool.getConnection();
             PreparedStatement ps = conn.prepareStatement(metricsQuery);
             ResultSet rs = ps.executeQuery()){
            Map<String, Integer> metrics = new LinkedHashMap<>();
            metrics.put("PENDING", 0);
            metrics.put("RUNNING", 0);
            metrics.put("COMPLETED", 0);
            metrics.put("FAILED", 0);
            while (rs.next()) {
                String status = rs.getString("status");
                int count = rs.getInt("task_count");
                metrics.put(status, count);
            }
            StringBuilder jsonBuilder = new StringBuilder("{");
            for (Map.Entry<String, Integer> entry : metrics.entrySet()) {
                jsonBuilder.append(String.format("\"%s\": %d, ", entry.getKey(), entry.getValue()));
            }
            if (!metrics.isEmpty()) {
                jsonBuilder.setLength(jsonBuilder.length() - 2); // Remove trailing comma and space
            }
            jsonBuilder.append("}");
            return jsonBuilder.toString();
        }catch (SQLException e){
            System.err.println("Failed to fetch metrics: " + e.getMessage());
            return "{\"error\": \"Failed to fetch metrics\"}";
        }
    }
}
