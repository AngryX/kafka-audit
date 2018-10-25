package com.github.kafka.audit

import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.lang.Exception

class MessageCountingProducerInterceptor<K,V>: ProducerInterceptor<K,V> {

    private val log = LoggerFactory.getLogger(MessageCountingProducerInterceptor::class.java)

    private val counting = MessageCounting()

    override fun configure(configs: Map<String, Any?>) {
        try {
            counting.configure(CountingConfigs(true, configs))
        } catch(e: MessageCountingException){
            log.error("Error while configuring of counting", e)
        }

    }

    override fun onSend(record: ProducerRecord<K, V>) = record

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if(exception != null || metadata == null){
            return
        }
        try {
            counting.add(metadata.topic(), metadata.timestamp())
        } catch(e: MessageCountingException){
            log.error("Error while adding new values", e)
        }
    }

    override fun close() {
        log.info("Closing of counting producer interceptor")
        counting.close()
    }

}