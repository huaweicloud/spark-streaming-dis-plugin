package org.apache.spark.streaming.dis

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import org.apache.spark.util.Utils
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

/**
  * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
  * logging messages at different levels using methods that only evaluate parameters lazily if the
  * log level is enabled.
  */
trait MyLogging {

  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  protected def myLogName = {
    // Ignore trailing $'s in the class names for Scala objects
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  protected def myLog: Logger = {
    if (log_ == null) {
      myInitializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(myLogName)
    }
    log_
  }

  // Log methods that take only a String
  protected def myLogInfo(msg: => String) {
    if (myLog.isInfoEnabled) myLog.info(msg)
  }

  protected def myLogDebug(msg: => String) {
    if (myLog.isDebugEnabled) myLog.debug(msg)
  }

  protected def myLogTrace(msg: => String) {
    if (myLog.isTraceEnabled) myLog.trace(msg)
  }

  protected def myLogWarning(msg: => String) {
    if (myLog.isWarnEnabled) myLog.warn(msg)
  }

  protected def myLogError(msg: => String) {
    if (myLog.isErrorEnabled) myLog.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def myLogInfo(msg: => String, throwable: Throwable) {
    if (myLog.isInfoEnabled) myLog.info(msg, throwable)
  }

  protected def myLogDebug(msg: => String, throwable: Throwable) {
    if (myLog.isDebugEnabled) myLog.debug(msg, throwable)
  }

  protected def myLogTrace(msg: => String, throwable: Throwable) {
    if (myLog.isTraceEnabled) myLog.trace(msg, throwable)
  }

  protected def myLogWarning(msg: => String, throwable: Throwable) {
    if (myLog.isWarnEnabled) myLog.warn(msg, throwable)
  }

  protected def myLogError(msg: => String, throwable: Throwable) {
    if (myLog.isErrorEnabled) myLog.error(msg, throwable)
  }

  protected def isMyTraceEnabled(): Boolean = {
    myLog.isTraceEnabled
  }

  protected def myInitializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    if (!MyLogging.initialized) {
      MyLogging.initLock.synchronized {
        if (!MyLogging.initialized) {
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      // scalastyle:off println
      if (!log4j12Initialized) {
        val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
        Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
          case Some(url) =>
            PropertyConfigurator.configure(url)
            System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
          case None =>
            System.err.println(s"Spark was unable to load $defaultLogProps")
        }
      }

      if (isInterpreter) {
        // Use the repl's main class to define the default log level when running the shell,
        // overriding the root logger's config if they're different.
        val rootLogger = LogManager.getRootLogger()
        val replLogger = LogManager.getLogger(myLogName)
        val replLevel = Option(replLogger.getLevel()).getOrElse(Level.WARN)
        if (replLevel != rootLogger.getEffectiveLevel()) {
          System.err.printf("Setting default log level to \"%s\".\n", replLevel)
          System.err.println("To adjust logging level use sc.setLogLevel(newLevel). " +
            "For SparkR, use setLogLevel(newLevel).")
          rootLogger.setLevel(replLevel)
        }
      }
      // scalastyle:on println
    }
    MyLogging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    myLog
  }
}

private object MyLogging {
  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}