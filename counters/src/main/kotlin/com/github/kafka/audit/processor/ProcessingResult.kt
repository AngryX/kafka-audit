package com.github.kafka.audit.processor

import com.github.kafka.audit.NumberOfMessages

data class ProcessingResult(
        val processorId: String = "",
        val count: NumberOfMessages,
        val processed: Boolean = true
)