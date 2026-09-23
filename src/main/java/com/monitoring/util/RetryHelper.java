package com.monitoring.util;

import io.vertx.core.json.JsonObject;

/**
 * Utility for exact geometric exponential backoff calculation.
 * Ensures attempt start times match exact specification:
 * For base=200ms, factor=2.0, maxAttempts=3:
 * Attempt 1: 0ms
 * Attempt 2: 200ms
 * Attempt 3: 600ms
 */
public final class RetryHelper {

    private RetryHelper() {}

    public static int getMaxAttempts(JsonObject retryPolicy) {
        if (retryPolicy == null) return 3;
        return retryPolicy.getInteger("maxAttempts", 3);
    }

    public static long getBaseDelayMs(JsonObject retryPolicy) {
        if (retryPolicy == null) return 200L;
        return retryPolicy.getLong("baseDelayMs", 200L);
    }

    public static double getBackoffFactor(JsonObject retryPolicy) {
        if (retryPolicy == null) return 2.0;
        return retryPolicy.getDouble("backoffFactor", 2.0);
    }

    /**
     * Calculates delay before launching attempt number {@code nextAttemptNumber} (where nextAttemptNumber >= 2).
     *
     * @param nextAttemptNumber 2 for second attempt, 3 for third attempt, etc.
     * @param retryPolicy configuration JsonObject
     * @return delay in milliseconds
     */
    public static long calculateDelayMs(int nextAttemptNumber, JsonObject retryPolicy) {
        if (nextAttemptNumber <= 1) return 0L;
        long baseDelay = getBaseDelayMs(retryPolicy);
        double factor = getBackoffFactor(retryPolicy);
        return Math.round(baseDelay * Math.pow(factor, nextAttemptNumber - 2));
    }
}
