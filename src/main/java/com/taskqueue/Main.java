package com.taskqueue;

import com.taskqueue.monitor.ObservabilityServer;
import com.taskqueue.producer.TaskProducer;
import com.taskqueue.worker.WorkerNode;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        System.out.println("Starting Distributed Task Queue System...");

        TaskProducer producer = new TaskProducer();
        WorkerNode worker = new WorkerNode();
        ObservabilityServer observability = new ObservabilityServer(8080);
        try{
            observability.start();
        } catch (IOException e){
            System.err.println("Failed to start observability server: " + e.getMessage());
        }

        System.out.println("Enqueueing tasks...");
        try {
            for (int i = 1; i <= 5; i++) {
                String payload = String.format("{\"task_id\": %d, \"data\": \"Sample data for task %d\"}", i, i);
                long id = producer.enqueue(payload);
                System.out.println("Enqueued Task ID: " + id);
            }
        } catch (SQLException e) {
            System.err.println("Failed to enqueue tasks: " + e.getMessage());
        }

        worker.registerShutdownHook();
        System.out.println("Starting Worker Node...");
        worker.start();
        System.out.println("Worker Node started. Polling for tasks...");
    }
}
