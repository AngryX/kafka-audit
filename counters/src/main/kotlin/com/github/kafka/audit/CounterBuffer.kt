package com.github.kafka.audit

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

interface CounterBuffer{

    fun next(number: MessageCount)

    fun getValues(): List<MessageCount>

}

class SimpleCounterBuffer: CounterBuffer {

    private val buffer = CacheBuilder.newBuilder()
            .maximumSize(5000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build<MessageCountKey, AtomicLong>(
                    object: CacheLoader<MessageCountKey, AtomicLong>(){
                        override fun load(key: MessageCountKey) = AtomicLong()
                    }
            )


    override fun next(number: MessageCount) {
        val acc = buffer.get(number.key)
        var value = acc.get()
        while(!acc.compareAndSet(value, value + number.value)){
            value = acc.get()
        }
    }

    override fun getValues() = buffer.asMap()
            .map { MessageCount(it.key, it.value.getAndSet(0))}
            .filter { it.value > 0 }



}