package com.github.kafka.audit

import com.infobip.kafka.audit.processor.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class MessageCounting: AutoCloseable {

    private val log = LoggerFactory.getLogger(MessageCounting::class.java)

    private val counting = AtomicReference<Counting>()

    fun configure(configs: CountingConfigs) {
        val counting = Counting(configs)
        if(!this.counting.compareAndSet(null, counting)){
            log.warn("Kafka message counting has been already initialized")
        } else {
            log.info("Initialization of {} kafka message counting", configs.getClientId())
            counting.start()
        }
    }

    fun add(topic: String,
            timestamp: Long = System.currentTimeMillis(),
            value: Long = 1,
            counterType: String = ""
    ) {
        val counting = this.counting.get()
        if(counting == null){
            throw MessageCountingException("MessageCounting component has to be configured before using")
        }
        counting.add(topic, timestamp, value, counterType)
    }

    override fun close() {
        counting.getAndSet(null)?.close()
    }

    private class Counting(val configs: CountingConfigs): AutoCloseable {

        private val log = LoggerFactory.getLogger(Counting::class.java)

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