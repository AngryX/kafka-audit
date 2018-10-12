package com.infobip.kafka.audit.processor

import com.github.kafka.audit.*
import com.github.kafka.audit.processor.MessageCountProcessor
import com.github.kafka.audit.processor.MessageCountProcessorFactory
import com.github.kafka.audit.processor.MessageCountProcessorSettings
import com.github.kafka.audit.processor.WrongSettingsTypeException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig

class CompositeMessageCountProcessorSettings(
        private val clientId: String,
        private val configs: CountingConfig
): KafkaMessageCountProcessorSettings {

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
            configs.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        } else {
            value
        }
    }
}

class CompositeMessageCountProcessorFactory: MessageCountProcessorFactory {

    private val factories = listOf(
            InMemoryMessageCountProcessorFactory(),
            KafkaMessageCountProcessorFactory()
    ).associateBy { it.processorId() }

    override fun processorId() = "composite"

    override fun create(settings: MessageCountProcessorSettings) = when(settings) {
        is CompositeMessageCountProcessorSettings -> CompositeMessageCountProcessor(
                settings.listOfProcessor()
                        .mapNotNull { factories[it] }
                        .map { it.create(settings) }
        )
        else -> throw WrongSettingsTypeException()
    }
}

class CompositeMessageCountProcessor(private val processors: List<MessageCountProcessor>): MessageCountProcessor {

    override fun processorId() = "composite"

    override fun handle(records: List<MessageCount>) = processors
            .flatMap {
                it.handle(records)
            }

    override fun close() {
        processors.forEach { it.close() }
    }
}