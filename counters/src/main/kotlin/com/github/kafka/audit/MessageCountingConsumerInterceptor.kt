package com.github.kafka.audit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class MessageCountingConsumerInterceptor<K, V>: ConsumerInterceptor<K, V> {

    private val log = LoggerFactory.getLogger(MessageCountingConsumerInterceptor::class.java)

    private val counting = MessageCounting()

    private val client = AtomicReference<String>()

    private val intervalDuration = AtomicLong()

    override fun onConsume(records: ConsumerRecords<K, V>): ConsumerRecords<K, V> {
        records.partitions().forEach { tp ->
            val clientData = KafkaClientData(
                    clientId = client.get() ?: "undefined",
                    topicName = tp.topic()
            )
            val auditIntervalDuration = intervalDuration.get()
            records.records(tp).forEach { record ->
                val count = MessageCount(
                        clientData,
                        record.timestamp() / auditIntervalDuration * auditIntervalDuration,
                        1L
                )
                counting.add(count)
            }
        }
        return records
    }

    override fun onCommit(offsets: MutableMap<TopicPartition, OffsetAndMetadata>) {

    }

    override fun configure(configs: Map<String, Any?>) {
        val countingConfig = CountingConfig(configs)
        intervalDuration.set(countingConfig.getIntervalDuration())
        client.set(countingConfig.getString(ConsumerConfig.GROUP_ID_CONFIG))
        counting.configure(countingConfig)
    }

    override fun close() {
        log.info("Closing of counting consumer interceptor")
        counting.close()
    }
}

