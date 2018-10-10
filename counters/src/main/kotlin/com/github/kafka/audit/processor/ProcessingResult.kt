package com.github.kafka.audit.processor

import com.github.kafka.audit.NumberOfMessages

data class ProcessingResult(
        val processorId: String = "",
        val number: NumberOfMessages,
        val processed: Boolean = true
)