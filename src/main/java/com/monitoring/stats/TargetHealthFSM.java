package com.monitoring.stats;

/**
 * Anti-flapping health state machine for monitored targets.
 */
public class TargetHealthFSM {

    public enum State {
        HEALTHY,
        DEGRADED,
        CRITICAL
    }

    public record Transition(boolean changed, State from, State to, String reason) {
        public static Transition noChange(State state) {
            return new Transition(false, state, state, null);
        }
    }

    private State state = State.HEALTHY;
    private int consecutiveFailures = 0;
    private int consecutiveSuccesses = 0;

    private final int failureThreshold;
    private final int recoveryThreshold;
    private final long degradedLatencyThresholdMs;

    public TargetHealthFSM() {
        this(3, 2, 1500L);
    }

    public TargetHealthFSM(int failureThreshold, int recoveryThreshold, long degradedLatencyThresholdMs) {
        this.failureThreshold = failureThreshold;
        this.recoveryThreshold = recoveryThreshold;
        this.degradedLatencyThresholdMs = degradedLatencyThresholdMs;
    }

    public Transition update(String status, long latencyMs, double avg1m) {
        boolean isSuccess = "SUCCESS".equalsIgnoreCase(status);

        if (!isSuccess) {
            consecutiveFailures++;
            consecutiveSuccesses = 0;

            if (consecutiveFailures >= failureThreshold && state != State.CRITICAL) {
                State previous = state;
                state = State.CRITICAL;
                return new Transition(true, previous, State.CRITICAL, failureThreshold + " consecutive check failures");
            }
            return Transition.noChange(state);
        }

        consecutiveSuccesses++;
        consecutiveFailures = 0;

        boolean isSlow = latencyMs >= degradedLatencyThresholdMs || avg1m >= degradedLatencyThresholdMs;

        if (state == State.CRITICAL) {
            if (consecutiveSuccesses >= recoveryThreshold) {
                State previous = state;
                state = isSlow ? State.DEGRADED : State.HEALTHY;
                String reason = isSlow
                        ? "Recovered to DEGRADED (response time >= " + degradedLatencyThresholdMs + "ms)"
                        : "Recovered after " + recoveryThreshold + " consecutive successful checks";
                return new Transition(true, previous, state, reason);
            }
            return Transition.noChange(state);
        }

        // Target was HEALTHY or DEGRADED
        if (isSlow && state != State.DEGRADED) {
            State previous = state;
            state = State.DEGRADED;
            return new Transition(true, previous, State.DEGRADED, "Latency exceeded " + degradedLatencyThresholdMs + "ms");
        }

        if (!isSlow && state == State.DEGRADED) {
            state = State.HEALTHY;
            return new Transition(true, State.DEGRADED, State.HEALTHY, "Latency returned to normal");
        }

        return Transition.noChange(state);
    }

    public State getState() {
        return state;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public int getConsecutiveSuccesses() {
        return consecutiveSuccesses;
    }

    public void reset() {
        this.state = State.HEALTHY;
        this.consecutiveFailures = 0;
        this.consecutiveSuccesses = 0;
    }
}