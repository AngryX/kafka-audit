package com.github.kafka.audit

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.concurrent.atomic.AtomicReference

class MessageCountingProducerInterceptor<K,V>: ProducerInterceptor<K,V> {

    private val log = LoggerFactory.getLogger(MessageCountingProducerInterceptor::class.java)

    private val counting = MessageCounting()

    private val client = AtomicReference<String>()

    override fun configure(configs: Map<String, Any?>) {
        val countingConfig = CountingConfigImpl(true, configs)
        client.set(countingConfig.getApplicationId())
        counting.configure(countingConfig)
    }

    override fun onSend(record: ProducerRecord<K, V>) = record

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if(exception != null || metadata == null){
            return
        }
        counting.add(
                KafkaClientData(
                        clientId = client.get() ?: "undefined",
                        topicName = metadata.topic(),
                        producer = true
                ),
                metadata.timestamp()
        )
    }

    override fun close() {
        log.info("Closing of {} counting producer interceptor", client.get())
        counting.close()
    }

}