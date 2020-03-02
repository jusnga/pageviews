package org.jusnga.pageviews;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Very basic task runner that atomically completes all tasks or not at all, doing this to keep things simple due to
 * time constraints. Ideally the kind of parallel tasks that go through this should go through something like yarn/spark.
 * @param <K>
 * @param <V>
 */
public class AtomicTaskRunner<K, V> implements AutoCloseable {
    private final ExecutorService taskRunner;

    private static final Logger logger = LoggerFactory.getLogger(AtomicTaskRunner.class);

    public AtomicTaskRunner(int numParallelTasks) {
        this.taskRunner = Executors.newFixedThreadPool(numParallelTasks);
    }

    public Map<K, V> runTasks(Map<K, Callable<V>> tasks) {
        Map<K, Future<V>> pendingTasks = tasks.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> taskRunner.submit(entry.getValue())
                ));

        Map<K, V> results = Maps.newHashMap();
        for (Map.Entry<K, Future<V>> entry: pendingTasks.entrySet()) {
            K key = entry.getKey();
            Future<V> pendingTask = entry.getValue();

            try {
                V result = pendingTask.get();
                results.put(key, result);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Task run failed for {}", key, e);
                throw new IllegalStateException("Error running task", e);
            }
        }

        return results;
    }

    // Stolen from https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
    @Override
    public void close() throws Exception {
        taskRunner.shutdown();
        try {
            if (!taskRunner.awaitTermination(60, TimeUnit.SECONDS)) {
                taskRunner.shutdownNow();
                if (!taskRunner.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            taskRunner.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
