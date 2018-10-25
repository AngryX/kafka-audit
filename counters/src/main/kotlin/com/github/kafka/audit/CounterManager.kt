package com.github.kafka.audit

import com.github.kafka.SimpleThreadFactory
import com.github.kafka.audit.processor.MessageCountProcessor
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class CounterManager (
        private val clientId: String,
        private val processor: MessageCountProcessor,
        private val buffer: CounterBuffer,
        auditPeriod: Duration = Duration.ofSeconds(2)
): Closeable {

    private val log = LoggerFactory.getLogger(CounterManager::class.java)

    private val executor = Executors.newSingleThreadScheduledExecutor(
            SimpleThreadFactory("kafka-counter-manager")
    )

    private val task = BufferTask(buffer, processor, executor, auditPeriod)

    fun start() {
        task.schedule()
    }

    override fun close() {
        log.info("Closing of {} kafka counter manager", clientId)
        task.close()
        executor.stop()
        val records = buffer.getValues()
        try{
            processor.handleAndReturnNotProcessed(records)
                    .forEach {
                        log.warn("Counter record {} was not processed", it)
                    }
        } catch(e: Exception){
            log.error("Error while handling counter records", e)
            records.forEach { log.warn("Counter record {} was not processed", it) }
        }
        processor.close()
    }

    private fun ExecutorService.stop(){
        executor.shutdown()
        try{
            if(!this.awaitTermination(5, TimeUnit.SECONDS)){
                val uncompleted = this.shutdownNow()
                log.warn("{} workers of kafka counter manager were not stopped correctly", uncompleted.size)
            } else {
                log.info("Kafka counter manager executor was stopped")
            }
        }catch (e: InterruptedException){
            log.error("Error while trying to shutdown kafka counter manager executor", e)
        }

    }
}

class BufferTask(
        private val buffer: CounterBuffer,
        private val processor: MessageCountProcessor,
        private val executor: ScheduledExecutorService,
        private val auditPeriod: Duration
): Runnable, Closeable {

    private val log = LoggerFactory.getLogger(BufferTask::class.java)
    private val closed = AtomicBoolean(false);


    override fun run() {
        if(closed.get()){
            return
        }
        val data = buffer.getValues()
        try {
            processor.handleAndReturnNotProcessed(data)
                    .forEach {
                        log.warn("Counter record {} was not processed", it)
                    }
            schedule()
        } catch (e: Exception){
            log.error("Error while trying to handle counters", e)
            repeat(data)
        }
    }

    override fun close() {
        closed.set(true)
    }

    fun schedule() {
        if(closed.get()){
            return
        }
        executor.schedule(
                this,
                auditPeriod.toMillis(),
                TimeUnit.MILLISECONDS
        )
    }



    fun repeat(data: List<MessageCount>, attempts: Long = 1) {
        if(closed.get()){
            log.warn("Attempt to repeat when task is closed")
            data.forEach {
                log.warn("Counter record {} was not handled", it)
            }
            return
        }
        executor.schedule(
                DataTask(data, attempts, processor, this),
                auditPeriod.toMillis() / 2,
                TimeUnit.MILLISECONDS
        )
    }

}

class DataTask(
        private val data: List<MessageCount>,
        private val attempts: Long,
        private val processor: MessageCountProcessor,
        private val bufferTask: BufferTask
): Runnable {

    private val log = LoggerFactory.getLogger(DataTask::class.java)

    override fun run() {
        log.warn("Attempt {} to handle counter records", attempts + 1)
        try {
            processor.handleAndReturnNotProcessed(data)
                    .forEach {
                        log.warn("Counter record {} was not processed", it)
                    }
            bufferTask.schedule()
        } catch (e: Exception){
            log.error("Error while trying to handle counters", e)
            bufferTask.repeat(data, attempts + 1)
        }

    }

}
