package com.infobip.kafka.audit.processor

import com.github.kafka.audit.MessageCount
import com.github.kafka.audit.MessageCountKey
import com.github.kafka.audit.processor.AbstractMessageCountProcessor
import com.github.kafka.audit.processor.MessageCountProcessorFactory
import com.github.kafka.audit.processor.MessageCountProcessorSettings
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class InMemoryMessageCountProcessorFactory: MessageCountProcessorFactory {

    override fun processorId() = "in_memory"

    override fun create(settings: MessageCountProcessorSettings) = InMemoryMessageCountProcessor(
            processorId()
    )

}
private val log = LoggerFactory.getLogger(InMemoryMessageCountProcessor::class.java)

private val cache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .removalListener<MessageCountKey, AtomicLong>{ notification ->
            log.info("InMemoryMessageCountProcessor record {}: {}", notification.key, notification.value)
        }
        .build<MessageCountKey, AtomicLong>(
                object: CacheLoader<MessageCountKey, AtomicLong>(){
                    override fun load(key: MessageCountKey) = AtomicLong()
                }
        )

class InMemoryMessageCountProcessor(processorId: String) : AbstractMessageCountProcessor(processorId) {

    fun getValue(key: MessageCountKey) = cache.get(key)?.get() ?: 0

    override fun handle(record: MessageCount): CompletableFuture<MessageCount> {
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