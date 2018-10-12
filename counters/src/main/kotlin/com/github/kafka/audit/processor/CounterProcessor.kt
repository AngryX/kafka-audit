package com.github.kafka.audit.processor

import com.github.kafka.audit.MessageCounter
import java.io.Closeable

interface CounterProcessor: Closeable {

    fun processorId(): String

    fun handle(numbers: List<MessageCounter>): List<ProcessingResult>

    fun handleAndReturnNotProcessed(numbers: List<MessageCounter>): List<MessageCounter> {
        return handle(numbers)
                .filter { !it.processed }
                .map { it.number }
    }

    override fun close() {}

}

interface CounterProcessorFactory {

    fun processorId(): String

    fun create(settings: CounterProcessorSettings): CounterProcessor

}

interface CounterProcessorSettings

class WrongSettingsTypeException: IllegalArgumentException("Wrong type of processor settings")