package com.github.kafka.audit.processor

import com.github.kafka.audit.NumberOfMessages
import java.io.Closeable

interface NumberProcessor: Closeable {

    fun processorId(): String

    fun handle(numbers: List<NumberOfMessages>): List<ProcessingResult>

    fun handleAndReturnNotProcessed(numbers: List<NumberOfMessages>): List<NumberOfMessages> {
        return handle(numbers)
                .filter { !it.processed }
                .map { it.number }
    }

    override fun close() {}

}

interface NumberProcessorFactory {

    fun processorId(): String

    fun create(settings: NumberProcessorSettings): NumberProcessor

}

interface NumberProcessorSettings

class WrongSettingsTypeException: IllegalArgumentException("Wrong type of processor settings")