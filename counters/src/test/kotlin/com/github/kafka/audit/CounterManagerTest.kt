package com.github.kafka.audit

import com.infobip.kafka.audit.processor.InMemoryCounterProcessor
import org.awaitility.Awaitility
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.Duration
import java.util.concurrent.TimeUnit

class CounterManagerTest {

    lateinit var processor: InMemoryCounterProcessor
    lateinit var buffer: CounterBuffer
    lateinit var manager: CounterManager

    @Before
    fun init(){
        processor = InMemoryCounterProcessor("test")
        buffer = SimpleCounterBuffer()
        manager = CounterManager(
                "test",
                processor,
                buffer,
                Duration.ofMillis(10)
        )
    }

    @After
    fun stop(){
        manager.close()
    }

    @Test
    fun `Should initiate processing of data saved in buffer`(){
        val count = 100L
        val key = MessageCounterKey(KafkaClientData(), 0)
        val counter = MessageCounter(key, count)
        buffer.next(counter)
        Awaitility.await()
                .atMost(1, TimeUnit.SECONDS)
                .until {
                    processor.getValue(key) == count
                }
    }
}