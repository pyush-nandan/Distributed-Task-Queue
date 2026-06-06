package com.taskqueue.model;

public class Task {
    private final long id;
    private final String payload;
    private final int retryCount;

    public Task(long id, String payload, int retryCount) {
        this.id = id;
        this.payload = payload;
        this.retryCount = retryCount;
    }

    public long getId() {
        return id;
    }

    public String getPayload() {
        return payload;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
