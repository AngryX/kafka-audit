package com.github.kafka.audit.processor

import com.github.kafka.audit.MessageCount
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

abstract class AbstractMessageCountProcessor(private val processorId: String): MessageCountProcessor {

    private val log = LoggerFactory.getLogger(AbstractMessageCountProcessor::class.java)

    override fun processorId() = processorId

    override fun handle(records: List<MessageCount>): List<ProcessingResult>{
        val futures = records.map {
            val future = CompletableFuture<ProcessingResult>()
            handleWithRetry(it, Long.MAX_VALUE, future)
            future
        }
        return CompletableFuture
                .allOf(*futures.toTypedArray())
                .thenApply { v -> futures.map { future -> future.join() } }
                .get()
    }

    private fun handleWithRetry(record: MessageCount, times: Long, future: CompletableFuture<ProcessingResult>){
        handle(record)
                .thenAccept{ future.complete(ProcessingResult(processorId(), record)) }
                .exceptionally { ex ->
                    log.error("Error while handling counter record {}", record, ex)
                    if (times <= 0) {
                        future.complete(ProcessingResult(processorId(), record, false))
                    } else {
                        handleWithRetry(record, times - 1, future)
                    }
                    null
                }
    }

    abstract fun handle(record: MessageCount): CompletableFuture<MessageCount>

}