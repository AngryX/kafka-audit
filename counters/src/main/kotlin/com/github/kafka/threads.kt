package com.github.kafka

import org.slf4j.LoggerFactory
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

class SimpleThreadFactory(private val threadName: String): ThreadFactory {

    private val log = LoggerFactory.getLogger(SimpleThreadFactory::class.java)

    private val counter = AtomicLong(1)

    private val uncaughtExceptionHandler = Thread.UncaughtExceptionHandler{ thread, throwable ->
        log.error("Uncaught error ${throwable.message} in ${thread.name} ")
    }

    override fun newThread(r: Runnable): Thread {
        val thread = Thread(r, "$threadName-${counter.andIncrement}")
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler)
        return thread
    }

}
