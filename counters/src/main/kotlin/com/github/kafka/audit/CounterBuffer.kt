package com.github.kafka.audit

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

interface CounterBuffer{

    fun next(number: MessageCounter)

    fun getValues(): List<MessageCounter>

}

class SimpleCounterBuffer: CounterBuffer {

    private val buffer = CacheBuilder.newBuilder()
            .maximumSize(5000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build<MessageCounterKey, AtomicLong>(
                    object: CacheLoader<MessageCounterKey, AtomicLong>(){
                        override fun load(key: MessageCounterKey) = AtomicLong()
                    }
            )


    override fun next(number: MessageCounter) {
        val acc = buffer.get(number.key)
        var value = acc.get()
        while(!acc.compareAndSet(value, value + number.value)){
            value = acc.get()
        }
    }

    override fun getValues() = buffer.asMap()
            .map { MessageCounter(it.key, it.value.getAndSet(0))}
            .filter { it.value > 0 }



}