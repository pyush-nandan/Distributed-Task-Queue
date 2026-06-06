package com.taskqueue.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public abstract class BaseIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:15").withInitScript("schema.sql");

    @BeforeAll
    static void setupPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setPassword(postgres.getPassword());
        config.setUsername(postgres.getUsername());
        config.setMaximumPoolSize(5);
        ConnectionPool.overrideDataSource(new HikariDataSource(config));
    }
}
