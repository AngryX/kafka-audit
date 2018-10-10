package com.infobip.kafka.audit.processor

import com.github.kafka.audit.ApplicationData
import com.github.kafka.audit.CounterConfig
import com.github.kafka.audit.NumberOfMessages
import com.github.kafka.audit.processor.NumberProcessor
import com.github.kafka.audit.processor.NumberProcessorFactory
import com.github.kafka.audit.processor.NumberProcessorSettings
import com.github.kafka.audit.processor.WrongSettingsTypeException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

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


class NumberProcessorConfig(original: Map<String, *>): AbstractConfig(
        ConfigDef()
                .define(APPLICATION_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_ID_DOC)
                .define(APPLICATION_LOCATION_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_LOCATION_DOC)
                .define(APPLICATION_INSTANCE_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, APPLICATION_INSTANCE_DOC)
                .define(TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, "audit-data", ConfigDef.Importance.HIGH, TOPIC_NAME_DOC)
                .define(LIST_OF_PROCESSORS_CONFIG, ConfigDef.Type.LIST, listOf("in_memory","kafka"), ConfigDef.Importance.HIGH, LIST_OF_PROCESSORS_DOC)
                .define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, emptyList<String>(), ConfigDef.Importance.MEDIUM, BOOTSTRAP_SERVERS_DOC),
        original,
        true
)

class CompositeNumberProcessorSettings(
        private val clientId: String,
        private val configs: NumberProcessorConfig,
        private val counterConfig: CounterConfig
): KafkaNumberProcessorSettings {

    fun listOfProcessor(): List<String> = configs.getList(LIST_OF_PROCESSORS_CONFIG)

    override fun applicationData() = ApplicationData(
            applicationId = configs.getString(APPLICATION_ID_CONFIG),
            instanceId = configs.getString(APPLICATION_INSTANCE_CONFIG),
            locationId = configs.getString(APPLICATION_LOCATION_CONFIG)
    )

    override fun auditTopicName() = configs.getString(TOPIC_NAME_CONFIG)

    override fun producerProperties() = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG to "audit_${clientId}"

    )

    private fun getBootstrapServers(): List<String>{
        val value = configs.getList(BOOTSTRAP_SERVERS_CONFIG)
        return if(value.isEmpty()){
            counterConfig.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        } else {
            value
        }
    }
}

class CompositeNumberProcessorFactory: NumberProcessorFactory {

    private val factories = listOf(
            InMemoryNumberProcessorFactory(),
            KafkaNumberProcessorFactory()
    ).associateBy { it.processorId() }

    override fun processorId() = "composite"

    override fun create(settings: NumberProcessorSettings) = when(settings) {
        is CompositeNumberProcessorSettings -> CompositeNumberProcessor(
                settings.listOfProcessor()
                        .mapNotNull { factories[it] }
                        .map { it.create(settings) }
        )
        else -> throw WrongSettingsTypeException()
    }
}

class CompositeNumberProcessor(private val processors: List<NumberProcessor>): NumberProcessor {

    override fun processorId() = "composite"

    override fun handle(records: List<NumberOfMessages>) = processors
            .flatMap {
                it.handle(records)
            }

    override fun close() {
        processors.forEach { it.close() }
    }
}