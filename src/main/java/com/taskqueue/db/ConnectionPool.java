package com.taskqueue.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.github.cdimascio.dotenv.Dotenv;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
    private static volatile HikariDataSource ds;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(requireEnv("DB_URL"));
        config.setUsername(requireEnv("DB_USER"));
        config.setPassword(requireEnv("DB_PASS"));
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(3000);
        config.setKeepaliveTime(30000);
        ds = new HikariDataSource(config);
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    private static String requireEnv(String key) throws IllegalStateException {
        try {
            Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
            String value = dotenv.get(key);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        } catch (Exception ignored) {}
        String value = System.getenv(key);
        if (value == null || value.isEmpty()){
            throw new IllegalStateException("Environment variable " + key + " is required but not set.");
        }
        return value;
    }

    static void overrideDataSource(HikariDataSource testDs){
        ds = testDs;
    }
}
