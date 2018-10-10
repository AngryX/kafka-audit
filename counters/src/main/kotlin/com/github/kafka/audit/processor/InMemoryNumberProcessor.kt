package com.infobip.kafka.audit.processor

import com.github.kafka.audit.KafkaClientData
import com.github.kafka.audit.NumberOfMessages
import com.github.kafka.audit.processor.AbstractNumberProcessor
import com.github.kafka.audit.processor.NumberProcessorFactory
import com.github.kafka.audit.processor.NumberProcessorSettings
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class InMemoryNumberProcessorFactory: NumberProcessorFactory {

    override fun processorId() = "in_memory"

    override fun create(settings: NumberProcessorSettings) = InMemoryNumberProcessor(
            processorId()
    )

}
private val log = LoggerFactory.getLogger(InMemoryNumberProcessor::class.java)

private val cache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .removalListener<Pair<KafkaClientData, Long>, AtomicLong>{ notification ->
            log.info("InMemoryNumberProcessor record {}: {}", notification.key, notification.value)
        }
        .build<Pair<KafkaClientData, Long>, AtomicLong>(
                object: CacheLoader<Pair<KafkaClientData, Long>, AtomicLong>(){
                    override fun load(key: Pair<KafkaClientData, Long>) = AtomicLong()
                }
        )

class InMemoryNumberProcessor(processorId: String) : AbstractNumberProcessor(processorId) {

    fun getValue(kafkaClient: KafkaClientData, intervalTimestamp: Long) = cache.get(Pair(kafkaClient, intervalTimestamp))

    override fun handle(record: NumberOfMessages): CompletableFuture<NumberOfMessages> {
        cache.get(Pair(record.kafkaClient, record.intervalTimestamp)).getAndAdd(record.value)
        return CompletableFuture.completedFuture(record)
    }

    override fun close() {
        super.close()
        print()
        cache.invalidateAll()
    }

    private fun print(){
        cache.asMap().forEach { log.info("Audit data {}", it) }
    }
}