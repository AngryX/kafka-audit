package com.infobip.kafka.audit.processor

import com.github.kafka.audit.MessageCounter
import com.github.kafka.audit.MessageCounterKey
import com.github.kafka.audit.processor.AbstractCounterProcessor
import com.github.kafka.audit.processor.CounterProcessorFactory
import com.github.kafka.audit.processor.CounterProcessorSettings
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class InMemoryCounterProcessorFactory: CounterProcessorFactory {

    override fun processorId() = "in_memory"

    override fun create(settings: CounterProcessorSettings) = InMemoryCounterProcessor(
            processorId()
    )

}
private val log = LoggerFactory.getLogger(InMemoryCounterProcessor::class.java)

private val cache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .removalListener<MessageCounterKey, AtomicLong>{ notification ->
            log.info("InMemoryCounterProcessor record {}: {}", notification.key, notification.value)
        }
        .build<MessageCounterKey, AtomicLong>(
                object: CacheLoader<MessageCounterKey, AtomicLong>(){
                    override fun load(key: MessageCounterKey) = AtomicLong()
                }
        )

class InMemoryCounterProcessor(processorId: String) : AbstractCounterProcessor(processorId) {

    fun getValue(key: MessageCounterKey) = cache.get(key)?.get() ?: 0

    override fun handle(record: MessageCounter): CompletableFuture<MessageCounter> {
        cache.get(record.key).getAndAdd(record.value)
        return CompletableFuture.completedFuture(record)
    }

    override fun close() {
        super.close()
        print()
        cache.invalidateAll()
    }

    private fun print(){
        cache.asMap().forEach { log.info("Counter {}", it) }
    }
}