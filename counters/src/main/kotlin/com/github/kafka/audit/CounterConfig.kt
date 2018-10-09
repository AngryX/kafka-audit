package com.github.kafka.audit

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import java.time.Duration

const val AUDIT_INTERVAL_DURATION_CONFIG = "audit.interval.duration"
const val AUDIT_INTERVAL_DURATION_DOC = "audit.interval.duration"

class CounterConfig(original: Map<String, *>): AbstractConfig(
        ConfigDef()
            .define(CommonClientConfigs.CLIENT_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
            .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(AUDIT_INTERVAL_DURATION_CONFIG, ConfigDef.Type.STRING, "PT15M", ConfigDef.Importance.HIGH, AUDIT_INTERVAL_DURATION_DOC),
        original,
        true) {

    fun getIntervalDuration() = Duration.parse(getString(AUDIT_INTERVAL_DURATION_DOC)).toMillis()

}