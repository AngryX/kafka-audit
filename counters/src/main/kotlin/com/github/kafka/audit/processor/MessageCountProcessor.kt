package com.github.kafka.audit.processor

import com.github.kafka.audit.CountingConfig
import com.github.kafka.audit.MessageCount
import java.io.Closeable

interface MessageCountProcessor: Closeable {

    fun processorId(): String

    fun handle(numbers: List<MessageCount>): List<ProcessingResult>

    fun handleAndReturnNotProcessed(numbers: List<MessageCount>): List<MessageCount> {
        return handle(numbers)
                .filter { !it.processed }
                .map { it.number }
    }

    override fun close() {}

}

interface MessageCountProcessorFactory {

    fun processorId(): String

    fun create(configs: CountingConfig): MessageCountProcessor

}