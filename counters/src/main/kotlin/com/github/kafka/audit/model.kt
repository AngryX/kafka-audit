package com.github.kafka.audit

data class ApplicationData (
        val applicationId: String = "",
        val instanceId: String = "",
        val locationId: String = ""
)

data class KafkaClientData(
        val clientId: String = "",
        val topicName: String,
        val producer: Boolean = false
)

data class NumberOfMessages(
        val application: ApplicationData,
        val kafkaClient: KafkaClientData,
        val counterType: String = "",
        val intervalTimestamp: Long,
        val value: Long = 0
){

    //todo: check keys and throw exception if there are not the same
    operator fun plus(other: NumberOfMessages) = this.copy(value = value + other.value)

    /*
    override fun compareTo(other: NumberOfMessages) =
            intervalTimestamp.compareTo(other.intervalTimestamp)

     */

}