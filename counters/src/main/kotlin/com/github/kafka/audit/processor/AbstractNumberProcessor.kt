package com.github.kafka.audit.processor

import com.github.kafka.audit.NumberOfMessages
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

abstract class AbstractNumberProcessor(private val processorId: String): NumberProcessor {

    private val log = LoggerFactory.getLogger(AbstractNumberProcessor::class.java)

    override fun processorId() = processorId

    override fun handle(records: List<NumberOfMessages>): List<ProcessingResult>{
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

    private fun handleWithRetry(record: NumberOfMessages, times: Long, future: CompletableFuture<ProcessingResult>){
        handle(record)
                .thenAccept{ future.complete(ProcessingResult(processorId(), record)) }
                .exceptionally { ex ->
                    log.error("Error while handling audit record {}", record, ex)
                    if (times <= 0) {
                        future.complete(ProcessingResult(processorId(), record, false))
                    } else {
                        handleWithRetry(record, times - 1, future)
                    }
                    null
                }
    }

    abstract fun handle(record: NumberOfMessages): CompletableFuture<NumberOfMessages>

}