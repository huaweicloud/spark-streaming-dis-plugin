package org.apache.spark.streaming.dis

trait BackOffExecution {
  val STOP: Long = -1
  
  /**
    * Return the number of milliseconds to wait before retrying the operation
    * or {@link #STOP} ({@value #STOP}) to indicate that no further attempt
    * should be made for the operation.
    */
  def nextBackOff: Long
}