package com.github.kafka.audit

data class KafkaClientData(
        val clientId: String = "",
        val topicName: String = "",
        val counterType: String = "",
        val producer: Boolean = false
)

typealias MessageCounterKey = Pair<KafkaClientData, Long>

data class MessageCounter(
        val kafkaClient: KafkaClientData = KafkaClientData(),
        val intervalTimestamp: Long = System.currentTimeMillis(),
        val value: Long = 0
){

    constructor(key: MessageCounterKey, value: Long): this(key.first, key.second, value)

    val key by lazy { MessageCounterKey(kafkaClient, intervalTimestamp) }

    //todo: check keys and throw exception if there are not the same
    operator fun plus(other: MessageCounter) = this.copy(value = value + other.value)

    /*
    override fun compareTo(other: MessageCounter) =
            intervalTimestamp.compareTo(other.intervalTimestamp)

     */

}

data class ApplicationData (
        val applicationId: String = "",
        val instanceId: String = "",
        val locationId: String = ""
)

data class ApplicationRecord (
       val application: ApplicationData = ApplicationData(),
       val number: MessageCounter = MessageCounter()
)