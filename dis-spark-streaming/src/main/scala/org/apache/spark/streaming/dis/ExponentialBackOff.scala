package org.apache.spark.streaming.dis

import com.huaweicloud.dis.exception.DISClientException

class ExponentialBackOff {
  /**
    * The default initial interval.
    */
  val DEFAULT_INITIAL_INTERVAL = 2000L

  /**
    * The default multiplier (increases the interval by 50%).
    */
  val DEFAULT_MULTIPLIER = 1.5

  /**
    * The default maximum back off time.
    */
  val DEFAULT_MAX_INTERVAL = 30000L

  /**
    * The default maximum elapsed time.
    */
  val DEFAULT_MAX_ELAPSED_TIME = Long.MaxValue

  private var initialInterval = DEFAULT_INITIAL_INTERVAL

  private var multiplier = DEFAULT_MULTIPLIER

  private var maxInterval = DEFAULT_MAX_INTERVAL

  private var maxElapsedTime = DEFAULT_MAX_ELAPSED_TIME

  /**
    * Create an instance with the supplied settings.
    *
    * @param initialInterval the initial interval in milliseconds
    * @param multiplier      the multiplier (should be greater than or equal to 1)
    */
  def this(initialInterval: Long, multiplier: Double) {
    this()
    checkMultiplier(multiplier)
    this.initialInterval = initialInterval
    this.multiplier = multiplier
  }

  /**
    * The initial interval in milliseconds.
    */
  def setInitialInterval(initialInterval: Long): Unit = {
    this.initialInterval = initialInterval
  }

  /**
    * Return the initial interval in milliseconds.
    */
  def getInitialInterval: Long = initialInterval

  /**
    * The value to multiply the current interval by for each retry attempt.
    */
  def setMultiplier(multiplier: Double): Unit = {
    checkMultiplier(multiplier)
    this.multiplier = multiplier
  }

  /**
    * Return the value to multiply the current interval by for each retry attempt.
    */
  def getMultiplier: Double = multiplier

  /**
    * The maximum back off time.
    */
  def setMaxInterval(maxInterval: Long): Unit = {
    this.maxInterval = maxInterval
  }

  /**
    * Return the maximum back off time.
    */
  def getMaxInterval: Long = maxInterval

  /**
    * The maximum elapsed time in milliseconds after which a call to
    */
  def setMaxElapsedTime(maxElapsedTime: Long): Unit = {
    this.maxElapsedTime = maxElapsedTime
  }

  /**
    * Return the maximum elapsed time in milliseconds after which a call to
    */
  def getMaxElapsedTime: Long = maxElapsedTime

  def start = new ExponentialBackOffExecution

  private def checkMultiplier(multiplier: Double) = {
    if (multiplier < 1) {
      throw new DISClientException("Invalid multiplier '" + multiplier + "'. Should be greater than or equal to 1." +
        " A multiplier of 1 is equivalent to a fixed interval.")
    }
  }

  class ExponentialBackOffExecution extends BackOffExecution {
    private var currentInterval: Long = -1
    private var currentElapsedTime: Long = 0

    override def nextBackOff: Long = {
      if (this.currentElapsedTime >= maxElapsedTime) return STOP
      val nextInterval = computeNextInterval
      this.currentElapsedTime += nextInterval
      nextInterval
    }

    private def computeNextInterval: Long = {
      val maxInterval: Long = getMaxInterval
      if (this.currentInterval >= maxInterval) return maxInterval
      else if (this.currentInterval < 0) {
        val initialInterval: Long = getInitialInterval
        this.currentInterval = if (initialInterval < maxInterval) {
          initialInterval
        }
        else {
          maxInterval
        }
      }
      else this.currentInterval = multiplyInterval(maxInterval)
      this.currentInterval
    }

    private def multiplyInterval(maxInterval: Long): Long = {
      var i: Long = this.currentInterval
      i = (i * getMultiplier).asInstanceOf[Number].longValue()
      if (i > maxInterval) {
        maxInterval
      }
      else {
        i
      }
    }

    override def toString: String = {
      val sb = new StringBuilder("ExponentialBackOff{")
      sb.append("currentInterval=").append(if (this.currentInterval < 0) "n/a"
      else this.currentInterval + "ms")
      sb.append(", multiplier=").append(getMultiplier)
      sb.append('}')
      sb.toString
    }
  }

}
