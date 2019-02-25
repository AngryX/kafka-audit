package com.infobip.kafka.audit.processor

import com.github.kafka.audit.*
import com.github.kafka.audit.processor.MessageCountProcessor
import com.github.kafka.audit.processor.MessageCountProcessorFactory

class CompositeMessageCountProcessorFactory: MessageCountProcessorFactory {

    private val factories = listOf(
            InMemoryMessageCountProcessorFactory(),
            KafkaMessageCountProcessorFactory()
    ).associateBy { it.processorId() }

    override fun processorId() = "composite"

    override fun create(configs: MessageCountingConfigs) = CompositeMessageCountProcessor(
            configs.listOfProcessor()
                    .mapNotNull { factories[it] }
                    .map { it.create(configs) }
    )

}

class CompositeMessageCountProcessor(private val processors: List<MessageCountProcessor>): MessageCountProcessor {

    override fun processorId() = "composite"

    override fun handle(records: List<MessageCount>) = processors
            .flatMap {
                it.handle(records)
            }

    override fun close() {
        processors.forEach { it.close() }
    }
}