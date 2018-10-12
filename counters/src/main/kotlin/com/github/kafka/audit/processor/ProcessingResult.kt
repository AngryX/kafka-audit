package com.github.kafka.audit.processor

import com.github.kafka.audit.MessageCounter

data class ProcessingResult(
        val processorId: String = "",
        val number: MessageCounter,
        val processed: Boolean = true
)