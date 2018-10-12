package com.github.kafka.audit

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import java.time.Duration

const val AUDIT_INTERVAL_DURATION_CONFIG = "audit.interval.duration"
const val AUDIT_INTERVAL_DURATION_DOC = "audit.interval.duration"
const val APPLICATION_ID_CONFIG = "audit.application.id"
const val APPLICATION_ID_DOC = "audit.application.id"
const val APPLICATION_INSTANCE_CONFIG = "audit.application.instance"
const val APPLICATION_INSTANCE_DOC = "audit.application.instance"
const val APPLICATION_LOCATION_CONFIG = "audit.application.location"
const val APPLICATION_LOCATION_DOC = "audit.application.location"
const val TOPIC_NAME_CONFIG = "audit.topic"
const val TOPIC_NAME_DOC = "audit.topic"
const val LIST_OF_PROCESSORS_CONFIG = "audit.processors"
const val LIST_OF_PROCESSORS_DOC = "audit.processors"
const val BOOTSTRAP_SERVERS_CONFIG = "audit.bootstrap.servers"
const val BOOTSTRAP_SERVERS_DOC = "audit.bootstrap.servers"

class CountingConfig(original: Map<String, *>): AbstractConfig(
        ConfigDef()
            .define(CommonClientConfigs.CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "defaultClientId", ConfigDef.Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
            .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
            .define(AUDIT_INTERVAL_DURATION_CONFIG, ConfigDef.Type.STRING, "PT15M", ConfigDef.Importance.HIGH, AUDIT_INTERVAL_DURATION_DOC)
            .define(APPLICATION_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_ID_DOC)
            .define(APPLICATION_LOCATION_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_LOCATION_DOC)
            .define(APPLICATION_INSTANCE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_INSTANCE_DOC)
            .define(TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, "kafka-counters", ConfigDef.Importance.HIGH, TOPIC_NAME_DOC)
            .define(LIST_OF_PROCESSORS_CONFIG, ConfigDef.Type.LIST, listOf("kafka"), ConfigDef.Importance.HIGH, LIST_OF_PROCESSORS_DOC)
            .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, emptyList<String>(), ConfigDef.Importance.MEDIUM, BOOTSTRAP_SERVERS_DOC),
        original,
        true
) {

    fun getClientId() = getString(CommonClientConfigs.CLIENT_ID_CONFIG)

    fun getIntervalDuration() = Duration.parse(getString(AUDIT_INTERVAL_DURATION_DOC)).toMillis()

}