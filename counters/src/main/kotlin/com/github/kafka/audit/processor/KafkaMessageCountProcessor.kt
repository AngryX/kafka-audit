package com.infobip.kafka.audit.processor

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.kafka.audit.ApplicationData
import com.github.kafka.audit.ApplicationRecord
import com.github.kafka.audit.MessageCount
import com.github.kafka.audit.processor.AbstractMessageCountProcessor
import com.github.kafka.audit.processor.MessageCountProcessorFactory
import com.github.kafka.audit.processor.MessageCountProcessorSettings
import com.github.kafka.audit.processor.WrongSettingsTypeException
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.concurrent.CompletableFuture

interface KafkaMessageCountProcessorSettings: MessageCountProcessorSettings {
    fun applicationData(): ApplicationData
    fun auditTopicName(): String
    fun producerProperties(): Map<String, Any>
}

class KafkaMessageCountProcessorFactory: MessageCountProcessorFactory {

    override fun processorId() = "kafka"

    override fun create(settings: MessageCountProcessorSettings) = when(settings) {
        is KafkaMessageCountProcessorSettings -> KafkaMessageCountProcessor(
                processorId(),
                settings.applicationData(),
                settings.auditTopicName(),
                settings.producerProperties()
        )
        else -> throw WrongSettingsTypeException()
    }

}

class KafkaMessageCountProcessor(processorId: String,
                                 private val applicationData: ApplicationData,
                                 private val auditTopicName: String,
                                 producerProperties: Map<String, Any>): AbstractMessageCountProcessor(processorId) {

    private val producer = KafkaProducer<ByteArray, ByteArray>(
            producerProperties.plus(listOf(
                    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
                    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1,
                    ProducerConfig.RETRIES_CONFIG to Integer.MAX_VALUE,
                    ProducerConfig.ACKS_CONFIG to "all",
                    ProducerConfig.LINGER_MS_CONFIG to 5,
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java
            ))
    )

    private val objectMapper = ObjectMapper().apply{
        configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
    }

    override fun handle(number: MessageCount): CompletableFuture<MessageCount> {
        val result = CompletableFuture<MessageCount>()
        try {
            val kafkaRecord = ProducerRecord(
                    auditTopicName,
                    objectMapper.writeValueAsBytes(number.kafkaClient),
                    objectMapper.writeValueAsBytes(ApplicationRecord(applicationData,number))
            )
            producer.send(kafkaRecord){ _, ex ->
                when(ex){
                    null -> result.complete(number)
                    else -> result.completeExceptionally(ex)
                }
            }
        } catch(ex: Exception){
            result.completeExceptionally(ex)
        }
        return result
    }

    override fun close() {
        producer.close()
    }

}