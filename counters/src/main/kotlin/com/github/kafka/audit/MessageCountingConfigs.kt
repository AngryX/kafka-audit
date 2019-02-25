package com.github.kafka.audit

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import java.time.Duration

const val APPLICATION_ID_CONFIG = "audit.application.id"
const val APPLICATION_INSTANCE_CONFIG = "audit.application.instance"
const val APPLICATION_LOCATION_CONFIG = "audit.application.location"
const val AUDIT_INTERVAL_DURATION_CONFIG = "audit.interval.duration"
const val LIST_OF_PROCESSORS_CONFIG = "audit.processors"

class MessageCountingConfigs(
        val producer: Boolean,
        private val original: Map<String, *>
){

    private val log = LoggerFactory.getLogger(MessageCountingConfigs::class.java)

    private val application: ApplicationData by lazy {
        ApplicationData(
                applicationId = getApplicationId(),
                instanceId = getStringValue(APPLICATION_INSTANCE_CONFIG),
                locationId = getStringValue(APPLICATION_LOCATION_CONFIG)
        )
    }

    private val duration: Long by lazy {
        Duration.parse(getStringValue(AUDIT_INTERVAL_DURATION_CONFIG, "PT15M")).toMillis()
    }

    fun getApplicationData() = application

    fun getApplicationId() = if(producer) {
        getStringValue(APPLICATION_ID_CONFIG)
    } else {
        getStringValue(ConsumerConfig.GROUP_ID_CONFIG)
    }

    fun getClientId() = getStringValue(CommonClientConfigs.CLIENT_ID_CONFIG)

    fun getIntervalDuration() = duration

    fun listOfProcessor(): List<String> = getListValue(LIST_OF_PROCESSORS_CONFIG, listOf("kafka"))

    fun getStringValue(name: String, defaultValue: String = "") = getValue(name, defaultValue)

    fun getListValue(name: String, defaultValue: List<String> = emptyList()) = getValue(name, defaultValue) { data ->
        when (data) {
            null -> null
            is String -> data.split(",").map { it.trim() }
            else -> {
                log.warn("Not expected type {} for property {} which should be transformed to list of strings", data.javaClass, name)
                null
            }
        }
    }

    private fun <T> getValue(
            name: String,
            defaultValue: T,
            transform: (Any?) -> T? = { it as T? }
    ) = original.getAndTransform(name,transform)
            .run{ this ?: defaultValue }

    private fun <T> Map<String, *>.getAndTransform(
            name: String,
            transform: (Any?) -> T?
    ) = original[name].run(transform)
}