package com.github.kafka.audit

import org.apache.kafka.clients.consumer.ConsumerInterceptor
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

class MessageCountingConsumerInterceptor<K, V>: ConsumerInterceptor<K, V> {

    private val log = LoggerFactory.getLogger(MessageCountingConsumerInterceptor::class.java)

    private val counting = MessageCounting()

    override fun configure(configs: Map<String, Any?>) {
        try {
            counting.configure(CountingConfigs(false, configs))
        } catch(e: MessageCountingException){
            log.error("Error while configuring of counting", e)
        }

    }

    override fun onConsume(records: ConsumerRecords<K, V>): ConsumerRecords<K, V> {
        records.partitions().forEach { tp ->
            records.records(tp).forEach { record ->
                try {
                    counting.add(tp.topic(), record.timestamp())
                } catch(e: MessageCountingException){
                    log.error("Error while adding new value", e)
                }
            }
        }
        return records
    }

    override fun onCommit(offsets: MutableMap<TopicPartition, OffsetAndMetadata>)  = Unit

    override fun close() {
        log.info("Closing of counting consumer interceptor")
        counting.close()
    }
}

