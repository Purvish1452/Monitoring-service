package com.monitoring.util;

import io.vertx.core.json.JsonObject;

/**
 * Helper for calculating exponential retry delays.
 */
public final class RetryHelper {

    private RetryHelper() {}

    public static int getMaxAttempts(JsonObject policy) {
        return policy != null ? policy.getInteger("maxAttempts", 3) : 3;
    }

    public static long getBaseDelayMs(JsonObject policy) {
        return policy != null ? policy.getLong("baseDelayMs", 200L) : 200L;
    }

    public static double getBackoffFactor(JsonObject policy) {
        return policy != null ? policy.getDouble("backoffFactor", 2.0) : 2.0;
    }

    public static long calculateDelayMs(int attempt, JsonObject policy) {
        if (attempt <= 1) {
            return 0L;
        }
        long baseDelay = getBaseDelayMs(policy);
        double factor = getBackoffFactor(policy);
        return Math.round(baseDelay * Math.pow(factor, attempt - 2));
    }
}