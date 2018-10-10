package com.github.kafka.audit

data class KafkaClientData(
        val clientId: String = "",
        val topicName: String = "",
        val counterType: String = "",
        val producer: Boolean = false
)

data class NumberOfMessages(
        val kafkaClient: KafkaClientData = KafkaClientData(),
        val intervalTimestamp: Long = System.currentTimeMillis(),
        val value: Long = 0
){

    //todo: check keys and throw exception if there are not the same
    operator fun plus(other: NumberOfMessages) = this.copy(value = value + other.value)

    /*
    override fun compareTo(other: NumberOfMessages) =
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
       val number: NumberOfMessages = NumberOfMessages()
)