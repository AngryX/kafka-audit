package com.infobip.kafka.audit.processor

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.kafka.audit.*
import com.github.kafka.audit.processor.AbstractMessageCountProcessor
import com.github.kafka.audit.processor.MessageCountProcessorFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.concurrent.CompletableFuture

class KafkaMessageCountProcessorFactory: MessageCountProcessorFactory {

    override fun processorId() = "kafka"

    override fun create(configs: CountingConfigs) = KafkaMessageCountProcessor(
                processorId(),
                configs.getApplicationData(),
                configs.auditTopicName(),
                configs.producerProperties()
        )

    private fun CountingConfigs.auditTopicName() = getStringValue(TOPIC_NAME_CONFIG)

    private fun CountingConfigs.producerProperties() = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to getBootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG to "audit_${getClientId()}",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 1,
            ProducerConfig.RETRIES_CONFIG to Integer.MAX_VALUE,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.LINGER_MS_CONFIG to 5,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java


    )

    private fun CountingConfigs.getBootstrapServers(): List<String>{
        val value = getListValue(BOOTSTRAP_SERVERS_CONFIG)
        return if(value.isEmpty()){
            getListValue(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        } else {
            value
        }
    }

}

class KafkaMessageCountProcessor(processorId: String,
                                 private val applicationData: ApplicationData,
                                 private val auditTopicName: String,
                                 producerProperties: Map<String, Any>): AbstractMessageCountProcessor(processorId) {

    private val producer = KafkaProducer<ByteArray, ByteArray>(producerProperties)

    private val objectMapper = ObjectMapper().apply{
        configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
    }

    override fun handle(count: MessageCount): CompletableFuture<MessageCount> {
        val result = CompletableFuture<MessageCount>()
        try {
            val kafkaRecord = ProducerRecord(
                    auditTopicName,
                    objectMapper.writeValueAsBytes(count.kafkaClient),
                    objectMapper.writeValueAsBytes(ApplicationRecord(applicationData, count))
            )
            producer.send(kafkaRecord){ _, ex ->
                when(ex){
                    null -> result.complete(count)
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