package com.infobip.kafka.audit.processor

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.kafka.audit.ApplicationData
import com.github.kafka.audit.ApplicationRecord
import com.github.kafka.audit.NumberOfMessages
import com.github.kafka.audit.processor.AbstractNumberProcessor
import com.github.kafka.audit.processor.NumberProcessorFactory
import com.github.kafka.audit.processor.NumberProcessorSettings
import com.github.kafka.audit.processor.WrongSettingsTypeException
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.concurrent.CompletableFuture

interface KafkaNumberProcessorSettings: NumberProcessorSettings {
    fun applicationData(): ApplicationData
    fun auditTopicName(): String
    fun producerProperties(): Map<String, Any>
}

class KafkaNumberProcessorFactory: NumberProcessorFactory {

    override fun processorId() = "kafka"

    override fun create(settings: NumberProcessorSettings) = when(settings) {
        is KafkaNumberProcessorSettings -> KafkaNumberProcessor(
                processorId(),
                settings.applicationData(),
                settings.auditTopicName(),
                settings.producerProperties()
        )
        else -> throw WrongSettingsTypeException()
    }

}

class KafkaNumberProcessor(processorId: String,
                              private val applicationData: ApplicationData,
                              private val auditTopicName: String,
                              producerProperties: Map<String, Any>): AbstractNumberProcessor(processorId) {

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

    override fun handle(number: NumberOfMessages): CompletableFuture<NumberOfMessages> {
        val result = CompletableFuture<NumberOfMessages>()
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