package com.github.kafka.audit

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

interface CounterBuffer {

    fun next(number: MessageCount)

    fun getValues(): List<MessageCount>

}

class SimpleCounterBuffer(
        maxSize: Long = 5000,
        expirationTimeInMinutes: Long = 30
): CounterBuffer {

    private val buffer = CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterAccess(expirationTimeInMinutes, TimeUnit.MINUTES)
            .build<MessageCountKey, AtomicLong>(
                    object: CacheLoader<MessageCountKey, AtomicLong>(){
                        override fun load(key: MessageCountKey) = AtomicLong()
                    }
            )


    override fun next(count: MessageCount) {
        val acc = buffer.get(count.key)
        var value = acc.get()
        while(!acc.compareAndSet(value, value + count.value)){
            value = acc.get()
        }
    }

    override fun getValues() = buffer.asMap()
            .map { MessageCount(it.key, it.value.getAndSet(0))}
            .filter { it.value > 0 }



}