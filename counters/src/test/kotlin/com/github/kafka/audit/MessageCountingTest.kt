package com.github.kafka.audit

import org.junit.Before
import org.junit.Test
import kotlin.test.assertTrue

class MessageCountingTest {

    lateinit var counting: MessageCounting

    @Before
    fun init(){
        counting = MessageCounting()
    }

    @Test
    fun `Should throw an MessageCountingException if adding happens before configuration`(){
        try{
            counting.add(topic = "test", timestamp = 0L)
            assertTrue(false, "MessageCountException should be thrown because counting has not been configured yet")
        } catch(e: MessageCountingException){
        } catch(e: Throwable){
            assertTrue(false, "Not expected type of exception")
        }

    }

    @Test
    fun `Should allow to close counting without configuration`(){
        try{
            counting.close()
        } catch(e: Throwable){
            assertTrue(false, "There should not be any exceptions")
        }

    }

}