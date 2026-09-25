package com.monitoring.stats;

import io.vertx.core.json.JsonObject;
import java.util.Arrays;

/**
 * Computes 1-minute and 5-minute rolling latency averages using a circular ring buffer.
 */
public class SlidingWindowStats {

    public static final int WINDOW_5M_SECONDS = 300;
    public static final int WINDOW_1M_SECONDS = 60;

    private final long[] timestamps = new long[WINDOW_5M_SECONDS];
    private final double[] totalLatencies = new double[WINDOW_5M_SECONDS];
    private final int[] sampleCounts = new int[WINDOW_5M_SECONDS];

    private double lastLatencyMs = 0.0;
    private long lastTimestampMs = 0L;
    private long totalSamples = 0L;

    public SlidingWindowStats() {
        Arrays.fill(timestamps, -1L);
    }

    public void record(long timestampMs, double latencyMs) {
        long second = timestampMs / 1000L;
        int idx = (int) Math.floorMod(second, WINDOW_5M_SECONDS);

        if (timestamps[idx] == second) {
            totalLatencies[idx] += latencyMs;
            sampleCounts[idx]++;
        } else {
            timestamps[idx] = second;
            totalLatencies[idx] = latencyMs;
            sampleCounts[idx] = 1;
        }

        this.lastLatencyMs = latencyMs;
        this.lastTimestampMs = timestampMs;
        this.totalSamples++;
    }

    public double getAverageLatency1m(long nowMs) {
        return calculateAverage(nowMs, WINDOW_1M_SECONDS);
    }

    public double getAverageLatency5m(long nowMs) {
        return calculateAverage(nowMs, WINDOW_5M_SECONDS);
    }

    public int getSampleCount1m(long nowMs) {
        return calculateSampleCount(nowMs, WINDOW_1M_SECONDS);
    }

    public int getSampleCount5m(long nowMs) {
        return calculateSampleCount(nowMs, WINDOW_5M_SECONDS);
    }

    public double getLastLatencyMs() {
        return lastLatencyMs;
    }

    public long getLastRecordedTimestampMs() {
        return lastTimestampMs;
    }

    public long getTotalSamplesRecorded() {
        return totalSamples;
    }

    public JsonObject toJsonObject(long nowMs) {
        return new JsonObject()
                .put("avg1m", round(getAverageLatency1m(nowMs)))
                .put("avg5m", round(getAverageLatency5m(nowMs)))
                .put("samples1m", getSampleCount1m(nowMs))
                .put("samples5m", getSampleCount5m(nowMs))
                .put("lastLatencyMs", round(lastLatencyMs))
                .put("lastCheckTimestamp", lastTimestampMs)
                .put("totalSamples", totalSamples);
    }

    private double calculateAverage(long nowMs, int windowSeconds) {
        long currentSec = nowMs / 1000L;
        long minSec = currentSec - windowSeconds + 1;

        double latencySum = 0.0;
        int count = 0;

        for (int i = 0; i < WINDOW_5M_SECONDS; i++) {
            long ts = timestamps[i];
            if (ts >= minSec && ts <= currentSec) {
                latencySum += totalLatencies[i];
                count += sampleCounts[i];
            }
        }

        return count > 0 ? latencySum / count : 0.0;
    }

    private int calculateSampleCount(long nowMs, int windowSeconds) {
        long currentSec = nowMs / 1000L;
        long minSec = currentSec - windowSeconds + 1;

        int count = 0;
        for (int i = 0; i < WINDOW_5M_SECONDS; i++) {
            long ts = timestamps[i];
            if (ts >= minSec && ts <= currentSec) {
                count += sampleCounts[i];
            }
        }
        return count;
    }

    private double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}