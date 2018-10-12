package com.github.kafka.audit

import com.infobip.kafka.audit.processor.*
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class MessageCounting: Closeable {

    private val log = LoggerFactory.getLogger(MessageCounting::class.java)

    private val manager = AtomicReference<CounterManager>()

    private val buffer = AtomicReference<CounterBuffer>()

    private val clientId = AtomicReference<String>()

    private val intervalDuration = AtomicLong()

    private val closed = AtomicBoolean(false)

    fun configure(configs: CountingConfig) {
        val buffer = SimpleCounterBuffer()
        if(!this.buffer.compareAndSet(null, buffer)){
            log.warn("Kafka message counting has been already initialized")
            return
        }
        log.info("Initialization of kafka message counting")
        clientId.set(configs.getClientId())
        intervalDuration.set(configs.getIntervalDuration())
        val processor = CompositeMessageCountProcessorFactory().create(CompositeMessageCountProcessorSettings(clientId.get(), configs))
        manager.set(
                CounterManager(
                    clientId.get(),
                    processor,
                    buffer,
                    Duration.ofSeconds(2) //todo: as parameter
                )
        )

    }

    fun add(client: KafkaClientData, timestamp: Long = System.currentTimeMillis(), value: Long = 1) {
        val auditIntervalDuration = intervalDuration.get()
        val intervalTime = timestamp / auditIntervalDuration * auditIntervalDuration
        val count = MessageCount(client, intervalTime, value)
        buffer.get()?.next(count)
    }

    override fun close() {
        if(closed.compareAndSet(false, true)){
            log.info("Closing of {} kafka message counting", clientId.get())
            manager.getAndSet(null)?.close()
        }
    }

}