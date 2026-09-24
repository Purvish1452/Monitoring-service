package com.monitoring.stats;

import io.vertx.core.json.JsonObject;

/**
 * Sliding window statistics engine using fixed-size primitive arrays.
 *
 * Uses a fixed 300-second ring buffer of primitive arrays to track 1-minute (60s)
 * and 5-minute (300s) rolling latency averages.
 * 
 * Bounded Memory Footprint:
 * - 300 long timestamps (2.4 KB)
 * - 300 double latencies (2.4 KB)
 * - 300 int sample counts (1.2 KB)
 * Total per target ~6 KB. No object creation on check recording.
 */
public class SlidingWindowStats {

    public static final int WINDOW_5M_SECONDS = 300;
    public static final int WINDOW_1M_SECONDS = 60;

    private final long[] bucketTimestamps = new long[WINDOW_5M_SECONDS];
    private final double[] bucketTotalLatency = new double[WINDOW_5M_SECONDS];
    private final int[] bucketSampleCount = new int[WINDOW_5M_SECONDS];

    private double lastLatencyMs = 0.0;
    private long lastRecordedTimestampMs = 0L;
    private long totalSamplesRecorded = 0L;

    public SlidingWindowStats() {
        // Initialize timestamps to -1 to signify empty buckets
        for (int i = 0; i < WINDOW_5M_SECONDS; i++) {
            bucketTimestamps[i] = -1L;
        }
    }

    /**
     * Records a probe latency sample using the fixed-size primitive ring buffer.
     * @param timestampMs Epoch timestamp in milliseconds
     * @param latencyMs   Observed latency in milliseconds
     */
    public void record(long timestampMs, double latencyMs) {
        long second = timestampMs / 1000L;
        int index = (int) (Math.floorMod(second, WINDOW_5M_SECONDS));

        if (bucketTimestamps[index] == second) {
            // Same second: accumulate latency and sample count
            bucketTotalLatency[index] += latencyMs;
            bucketSampleCount[index] += 1;
        } else {
            // New second occupying this slot: overwrite bucket
            bucketTimestamps[index] = second;
            bucketTotalLatency[index] = latencyMs;
            bucketSampleCount[index] = 1;
        }

        this.lastLatencyMs = latencyMs;
        this.lastRecordedTimestampMs = timestampMs;
        this.totalSamplesRecorded++;
    }

    /**
     * Calculates the rolling average latency over the last 1 minute (60 seconds).
     *
     * @param currentTimestampMs Current epoch timestamp in milliseconds
     * @return 1-minute average latency in ms, or 0.0 if no samples in window
     */
    public double getAverageLatency1m(long currentTimestampMs) {
        return calculateAverage(currentTimestampMs, WINDOW_1M_SECONDS);
    }

    /**
     * Calculates the rolling average latency over the last 5 minutes (300 seconds).
     *
     * @param currentTimestampMs Current epoch timestamp in milliseconds
     * @return 5-minute average latency in ms, or 0.0 if no samples in window
     */
    public double getAverageLatency5m(long currentTimestampMs) {
        return calculateAverage(currentTimestampMs, WINDOW_5M_SECONDS);
    }

    /**
     * Returns the number of samples recorded within the last 1 minute (60 seconds).
     */
    public int getSampleCount1m(long currentTimestampMs) {
        return calculateSampleCount(currentTimestampMs, WINDOW_1M_SECONDS);
    }

    /**
     * Returns the number of samples recorded within the last 5 minutes (300 seconds).
     */
    public int getSampleCount5m(long currentTimestampMs) {
        return calculateSampleCount(currentTimestampMs, WINDOW_5M_SECONDS);
    }

    public double getLastLatencyMs() {
        return lastLatencyMs;
    }

    public long getLastRecordedTimestampMs() {
        return lastRecordedTimestampMs;
    }

    public long getTotalSamplesRecorded() {
        return totalSamplesRecorded;
    }

    /**
     * Encodes the sliding window statistics to a JsonObject.
     */
    public JsonObject toJsonObject(long currentTimestampMs) {
        return new JsonObject()
                .put("avg1m", roundTwoDecimals(getAverageLatency1m(currentTimestampMs)))
                .put("avg5m", roundTwoDecimals(getAverageLatency5m(currentTimestampMs)))
                .put("samples1m", getSampleCount1m(currentTimestampMs))
                .put("samples5m", getSampleCount5m(currentTimestampMs))
                .put("lastLatencyMs", roundTwoDecimals(lastLatencyMs))
                .put("lastCheckTimestamp", lastRecordedTimestampMs)
                .put("totalSamples", totalSamplesRecorded);
    }

    private double calculateAverage(long currentTimestampMs, int windowSeconds) {
        long currentSecond = currentTimestampMs / 1000L;
        long minSecond = currentSecond - windowSeconds + 1;

        double totalLatency = 0.0;
        int totalSamples = 0;

        for (int i = 0; i < WINDOW_5M_SECONDS; i++) {
            long ts = bucketTimestamps[i];
            if (ts >= minSecond && ts <= currentSecond) {
                totalLatency += bucketTotalLatency[i];
                totalSamples += bucketSampleCount[i];
            }
        }

        if (totalSamples == 0) {
            return 0.0;
        }

        return totalLatency / totalSamples;
    }

    private int calculateSampleCount(long currentTimestampMs, int windowSeconds) {
        long currentSecond = currentTimestampMs / 1000L;
        long minSecond = currentSecond - windowSeconds + 1;

        int totalSamples = 0;
        for (int i = 0; i < WINDOW_5M_SECONDS; i++) {
            long ts = bucketTimestamps[i];
            if (ts >= minSecond && ts <= currentSecond) {
                totalSamples += bucketSampleCount[i];
            }
        }
        return totalSamples;
    }

    private double roundTwoDecimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
