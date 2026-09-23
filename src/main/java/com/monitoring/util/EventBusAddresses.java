package com.monitoring.util;

/**
 * Centralized registry of Vert.x EventBus address constants.
 * Components communicate strictly over these addresses using JsonObject/JsonArray payloads.
 */
public final class EventBusAddresses {

    private EventBusAddresses() {}

    // Target Management APIs
    public static final String TARGET_REGISTER = "api.target.register";
    public static final String TARGET_REGISTER_BULK = "api.target.register.bulk";
    public static final String TARGET_DELETE = "api.target.delete";
    public static final String TARGET_GET = "api.target.get";
    public static final String TARGET_LIST = "api.target.list";
    public static final String TARGET_BULK_CHECK = "api.target.bulk-check";

    // Scheduler Control
    public static final String SCHEDULER_TARGET_ADD = "scheduler.target.add";
    public static final String SCHEDULER_TARGET_REMOVE = "scheduler.target.remove";
    public static final String SCHEDULER_PAUSE = "scheduler.pause";

    // Probing & Execution
    public static final String CHECK_EXECUTE = "check.execute";
    public static final String CHECK_COMPLETED = "check.completed";
    public static final String CHECK_RESULT = "check.result";

    // Stats & Alerting
    public static final String STATS_GET = "api.stats.get";
    public static final String ALERTS_LIST = "api.alerts.list";
    public static final String ALERT_EVENTS = "monitor.alert.events";

    // Persistence Worker
    public static final String AUDIT_LOG = "monitor.audit.log";
    public static final String PERSIST_TARGET_SAVE = "persist.target.save";
    public static final String PERSIST_TARGET_DELETE = "persist.target.delete";
    public static final String PERSIST_TARGET_LOAD = "persist.target.load";
}
