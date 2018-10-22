package com.github.kafka.audit

import com.infobip.kafka.audit.processor.*
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class MessageCounting: Closeable {

    private val log = LoggerFactory.getLogger(MessageCounting::class.java)

    private val manager = AtomicReference<CounterManager>()

    private val buffer = AtomicReference<CounterBuffer>()

    private val configs = AtomicReference<CountingConfig>()

    private val closed = AtomicBoolean(false)

    fun configure(configs: CountingConfig) {
        val buffer = SimpleCounterBuffer()
        if(!this.buffer.compareAndSet(null, buffer)){
            log.warn("Kafka message counting has been already initialized")
            return
        }
        log.info("Initialization of {} kafka message counting", configs.getClientId())
        this.configs.set(configs)
        val processor = CompositeMessageCountProcessorFactory().create(configs)
        manager.set(
                CounterManager(
                    configs.getClientId(),
                    processor,
                    buffer,
                    Duration.ofSeconds(2) //todo: as parameter
                )
        )

    }

    fun add(client: KafkaClientData, timestamp: Long = System.currentTimeMillis(), value: Long = 1) {
        val auditIntervalDuration = configs.get().getIntervalDuration()
        val intervalTime = timestamp / auditIntervalDuration * auditIntervalDuration
        val count = MessageCount(client, intervalTime, value)
        buffer.get()?.next(count)
    }

    override fun close() {
        if(closed.compareAndSet(false, true)){
            log.info("Closing of {} kafka message counting", configs.get().getClientId())
            manager.getAndSet(null)?.close()
        }
    }

}