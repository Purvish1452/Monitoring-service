package com.monitoring.stats;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlidingWindowStatsTest {

    private SlidingWindowStats stats;

    @BeforeEach
    void setUp() {
        stats = new SlidingWindowStats();
    }

    @Test
    @DisplayName("1. Verify initial empty state returns 0.0 average and 0 samples")
    void testInitialEmptyState() {
        long now = 1000000000L;
        assertEquals(0.0, stats.getAverageLatency1m(now));
        assertEquals(0.0, stats.getAverageLatency5m(now));
        assertEquals(0, stats.getSampleCount1m(now));
        assertEquals(0, stats.getSampleCount5m(now));
        assertEquals(0L, stats.getTotalSamplesRecorded());
    }

    @Test
    @DisplayName("2. Verify AC 5 Scenario A: 30s @ 10ms + 30s @ 51ms -> 1m average is EXACTLY 30.50 ms")
    void testAc5ScenarioA_OneMinuteAverage() {
        long baseTimestamp = 1700000000_000L; // Starting epoch ms

        // First 30 seconds: 1 check per second at 10ms latency
        for (int i = 0; i < 30; i++) {
            long ts = baseTimestamp + (i * 1000L);
            stats.record(ts, 10.0);
        }

        // Next 30 seconds: 1 check per second at 51ms latency
        for (int i = 30; i < 60; i++) {
            long ts = baseTimestamp + (i * 1000L);
            stats.record(ts, 51.0);
        }

        // Query at the end of the 60-second window (second 59)
        long currentTimestamp = baseTimestamp + (59 * 1000L);

        assertEquals(60, stats.getSampleCount1m(currentTimestamp));
        
        // Exact math: (30 * 10.0 + 30 * 51.0) / 60 = (300.0 + 1530.0) / 60 = 1830.0 / 60 = 30.50
        double avg1m = stats.getAverageLatency1m(currentTimestamp);
        assertEquals(30.50, avg1m, 0.0001, "1-minute average must equal exactly 30.50 ms");

        JsonObject json = stats.toJsonObject(currentTimestamp);
        assertEquals(30.50, json.getDouble("avg1m"));
        assertEquals(60, json.getInteger("samples1m"));
    }

    @Test
    @DisplayName("3. Verify AC 5 Scenario B: 240s @ 80ms + 60s @ 43.75ms -> 5m average is EXACTLY 72.75 ms")
    void testAc5ScenarioB_FiveMinuteAverage() {
        long baseTimestamp = 1700000000_000L;

        // 4 minutes (240 seconds): 1 check per second at 80.0 ms
        for (int i = 0; i < 240; i++) {
            long ts = baseTimestamp + (i * 1000L);
            stats.record(ts, 80.0);
        }

        // 1 minute (60 seconds): 1 check per second at 43.75 ms
        for (int i = 240; i < 300; i++) {
            long ts = baseTimestamp + (i * 1000L);
            stats.record(ts, 43.75);
        }

        // Query at the end of the 300-second window (second 299)
        long currentTimestamp = baseTimestamp + (299 * 1000L);

        assertEquals(300, stats.getSampleCount5m(currentTimestamp));

        // Exact math: (240 * 80.0 + 60 * 43.75) / 300 = (19200.0 + 2625.0) / 300 = 21825.0 / 300 = 72.75
        double avg5m = stats.getAverageLatency5m(currentTimestamp);
        assertEquals(72.75, avg5m, 0.0001, "5-minute average must equal exactly 72.75 ms");

        // The 1-minute window at this point only contains the last 60 seconds (43.75ms)
        double avg1m = stats.getAverageLatency1m(currentTimestamp);
        assertEquals(43.75, avg1m, 0.0001, "1-minute average at this point must equal exactly 43.75 ms");

        JsonObject json = stats.toJsonObject(currentTimestamp);
        assertEquals(72.75, json.getDouble("avg5m"));
        assertEquals(43.75, json.getDouble("avg1m"));
        assertEquals(300, json.getInteger("samples5m"));
        assertEquals(60, json.getInteger("samples1m"));
    }

    @Test
    @DisplayName("4. Verify rolling window expiration and ring buffer slot reuse")
    void testRollingWindowExpiration() {
        long baseTimestamp = 1000_000L;

        // Record check at t = 0s with 100ms
        stats.record(baseTimestamp, 100.0);

        // At t = 10s: 1m avg = 100ms
        assertEquals(100.0, stats.getAverageLatency1m(baseTimestamp + 10_000L));

        // At t = 61s: older than 60s window -> 1m avg should expire and be 0.0
        assertEquals(0.0, stats.getAverageLatency1m(baseTimestamp + 61_000L));
        // But 5m avg should still include it
        assertEquals(100.0, stats.getAverageLatency5m(baseTimestamp + 61_000L));

        // At t = 301s: older than 300s window -> 5m avg should also expire
        assertEquals(0.0, stats.getAverageLatency5m(baseTimestamp + 301_000L));
    }

    @Test
    @DisplayName("5. Verify multiple checks within the same second accumulate correctly")
    void testMultipleChecksInSameSecond() {
        long timestamp = 1700000000_000L;

        // 3 checks in the same second: 10ms, 20ms, 30ms (avg = 20ms)
        stats.record(timestamp, 10.0);
        stats.record(timestamp + 100L, 20.0);
        stats.record(timestamp + 200L, 30.0);

        assertEquals(3, stats.getSampleCount1m(timestamp));
        assertEquals(20.0, stats.getAverageLatency1m(timestamp), 0.0001);
        assertEquals(3L, stats.getTotalSamplesRecorded());
    }

    @Test
    @DisplayName("6. Verify high-throughput execution with bounded memory (100,000 records)")
    void testHighThroughputStress() {
        long start = 1700000000_000L;

        for (int i = 0; i < 100_000; i++) {
            stats.record(start + (i * 50L), 25.0); // 20 checks/sec
        }

        long finalTs = start + (99_999 * 50L);
        assertEquals(25.0, stats.getAverageLatency1m(finalTs), 0.0001);
        assertEquals(25.0, stats.getAverageLatency5m(finalTs), 0.0001);
        assertEquals(100_000L, stats.getTotalSamplesRecorded());
    }
}
