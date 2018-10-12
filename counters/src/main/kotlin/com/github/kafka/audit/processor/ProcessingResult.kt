package com.github.kafka.audit.processor

import com.github.kafka.audit.MessageCount

data class ProcessingResult(
        val processorId: String = "",
        val number: MessageCount,
        val processed: Boolean = true
)