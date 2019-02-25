package com.github.kafka.audit

import com.infobip.kafka.audit.processor.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class MessageCounting: AutoCloseable {

    private val log = LoggerFactory.getLogger(MessageCounting::class.java)

    private val handler = AtomicReference<Handler>()

    fun configure(configs: MessageCountingConfigs) {
        val handler = Handler(configs)
        if(!this.handler.compareAndSet(null, handler)){
            log.warn("Kafka message counting has been already initialized")
        } else {
            log.info("Initialization of {} kafka message counting", configs.getClientId())
            handler.start()
        }
    }

    fun add(topic: String,
            timestamp: Long,
            value: Long = 1,
            counterType: String = ""
    ) {
        val handler = this.handler.get()
        handler?.add(topic, timestamp, value, counterType)
                ?: throw MessageCountingException("MessageCounting component has to be configured before using")
    }

    override fun close() {
        handler.getAndSet(null)?.close()
    }

    private class Handler(val configs: MessageCountingConfigs): AutoCloseable {

        private val log = LoggerFactory.getLogger(Handler::class.java)

        private val buffer = SimpleCounterBuffer()

        private val manager by lazy {
            CounterManager(
                    configs.getClientId(),
                    CompositeMessageCountProcessorFactory().create(configs),
                    buffer,
                    Duration.ofSeconds(2) //todo: as parameter
            )
        }

        fun start(){
            manager.start()
        }

        fun add(topic: String, timestamp: Long, value: Long, counterType: String){
            val auditIntervalDuration = configs.getIntervalDuration()
            val intervalTime = timestamp / auditIntervalDuration * auditIntervalDuration
            val client = KafkaClientData(
                    clientId = configs.getApplicationId(),
                    topicName = topic,
                    counterType = counterType,
                    producer = configs.producer
            )
            buffer.next(MessageCount(client, intervalTime, value))
        }

        override fun close() {
            log.info("Closing of {} kafka message counting", configs.getClientId())
            manager.close()
        }

    }
}