/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2019 All Rights Reserved.
 */
package org.apache.spark.util

import java.net.InetAddress
import java.util._
import java.util.concurrent.{Executors, ScheduledExecutorService}

import com.bilibili.config.DeployConfig
import com.bilibili.config.transport.ConfigTransportConfig
import com.bilibili.config.transport.bean.{CheckParams, ConfVersion, GetParams}
import com.bilibili.config.transport.impl.ConfigServiceImpl
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

/**
 *
 * @author
 * @version
 */
object ConfigurationUtil extends Logging {

  private val EMPTY = ""
  private val config = new Properties
  private var oldConfig = new Properties
  private var needRefreshConfiguration = true

  /**
   * 时间一分钟
   */
  private[util] val FIVE_MINUTE = 5 * 60 * 1000L
  /**
   * 刷新线程
   */
  private var executor: ScheduledExecutorService = _

  def initConfiguration(): Unit = {
    logInfo(System.getenv.toString)
    printEnvironment(System.getenv)
    try {
      logInfo(">>> Fetching configuration files ...")
      fetch(System.getenv)
    } catch {
      case e: Exception =>
        logError("Fetching error", e)
    }
    oldConfig = config.clone.asInstanceOf[Properties]
    executor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("refresh configuration-%d").build)
    executor.submit(new RefreshConfigThread)
  }

  def refreshConfiguration(): Unit = {
    try {
      logInfo(">>> Refresh dispatcher configuration ...")
      fetch(System.getenv)
    } catch {
      case e: Exception =>
        logError("Fetching error", e)
    }
    if (config == oldConfig) {
      needRefreshConfiguration = false
    } else {
      needRefreshConfiguration = true
      oldConfig = config.clone.asInstanceOf[Properties]
    }
  }

  def checkIfNeedRefreshConfiguration: Boolean = needRefreshConfiguration

  def setNeedRefreshConfiguration(signal: Boolean): Unit = needRefreshConfiguration = signal

  def getSparkCenterConfiguration: Properties = config

  private def printEnvironment(env: java.util.Map[String, String]) = {
    val names = Array("CONF_USE_CONF", "CONF_VERSION", "CONF_HOST", "CONF_PATH", "CONF_RELOAD", "CONF_TOKEN",
      "TREE_ID", "DEPLOY_ENV", "ZONE", "spring.config.location", "SPRING_CONFIG_LOCATION")
    logInfo("Environment:")
    for (name <- names) {
      logInfo(name + "=" + get(env, name))
    }
  }


  private[util] def fetch(env: java.util.Map[String, String]) = {
    try {
    var configLocation = get(env, "spring.config.location")
    configLocation = if (isBlank(configLocation)) {
      get(env, "SPRING_CONFIG_LOCATION")
    } else {
      configLocation
    }
    if (isBlank(configLocation)) {
      throw new RuntimeException("spring.config.location and SPRING_CONFIG_LOCATION not set")
    }
      val transportConfig = new ConfigTransportConfig
    val configHost = get(env, "CONF_HOST")
    if (!isBlank(configHost)) transportConfig.setServiceUrl("http://" + configHost)
    val deployConfig = new DeployConfig
    deployConfig.setService(
      Array[String](get(env, "TREE_ID"),
        get(env, "DEPLOY_ENV"), get(env, "ZONE"))
        .mkString("_"))
    deployConfig.setBuild(get(env, "CONF_VERSION"))
    deployConfig.setToken(get(env, "CONF_TOKEN"))
    deployConfig.setTmpConfDirectory(get(env, "CONF_PATH"))
    deployConfig.setConfLocation(configLocation)
    val localhost = InetAddress.getLocalHost
    deployConfig.setHostname(localhost.getHostName)
    deployConfig.setIp(localhost.getHostAddress)
    val configService: ConfigServiceImpl = new ConfigServiceImpl(transportConfig)
    val getParams = buildGetParams(deployConfig)
    val checkParams = buildCheckParams(deployConfig)
    val currentVersion = new java.util.concurrent.atomic.AtomicReference[ConfVersion]
    val latestVersion = currentVersion.get
    if (latestVersion != null) checkParams.setVersion(latestVersion.getVersion)
    val configMap = new java.util.HashMap[String, String]
    val remoteVersion = configService.check(checkParams)
    if (remoteVersion != null && !remoteVersion.isSameVersion(checkParams.getVersion)) {
      getParams.setVersion(remoteVersion.getVersion)
      getParams.setIds(remoteVersion.getDiffs)
      var configMapTmp = configService.get(getParams)
      configMap.putAll(configMapTmp)
    }
    putPopToConfig(configMap, "elephant.properties")
    } catch {
      case e: Exception =>
        logError("Fetching error", e)
    }
  }

  private def putPopToConfig(configMap: java.util.Map[String, String], `type`: String) = {
    config.clear()
    val configValue = configMap.get(`type`)
    val list = configValue.split("\n").toList
    val iterator = list.iterator
    while (iterator.hasNext) {
      val configItem = iterator.next
      val configvalues = configItem.split("=")
      val point = configItem.substring(0, configItem.indexOf("="))
      config.setProperty(configvalues(0), configItem.substring(point.length + 1, configItem.length))
    }
  }

  private def buildCheckParams(deployConfig: DeployConfig) = {
    val params = new CheckParams
    params.setService(deployConfig.getService)
    params.setBuild(deployConfig.getBuild)
    params.setHostname(deployConfig.getHostname)
    params.setIp(deployConfig.getIp)
    params.setToken(deployConfig.getToken)
    params
  }

  private def buildGetParams(deployConfig: DeployConfig) = {
    val params = new GetParams
    params.setService(deployConfig.getService)
    params.setBuild(deployConfig.getBuild)
    params.setHostname(deployConfig.getHostname)
    params.setIp(deployConfig.getIp)
    params.setToken(deployConfig.getToken)
    params
  }

  private def isBlank(s: String) = StringUtils.isBlank(s)

  private def get(map: java.util.Map[String, String], name: String) = map.getOrDefault(name, EMPTY)

  /**
    * Stop the cleaning thread and wait until the thread has finished running its current task.
    */
  def stop(): Unit = {
    logInfo("Shutting down ConfigurationUtil executor runner.")
    try {
      executor.shutdownNow()
    } catch {
      case e: InterruptedException =>
        logWarning("Interrupted while ConfigurationUtil executor to shutdown.", e)
    }
  }
}

/**
 * 每一分钟清理一次过期缓存
 */
class RefreshConfigThread extends Runnable with Logging {

  override def run(): Unit = {
    try {
      while (true) {
        Thread.sleep(ConfigurationUtil.FIVE_MINUTE)
        val switch = ConfigurationUtil
          .getSparkCenterConfiguration.getProperty("spark_on_off", "off")
        if (switch.equals("on")) {
          ConfigurationUtil.refreshConfiguration()
        }
      }
    } catch {
      case e: InterruptedException => logInfo("refresh config thread interrupted ", e)
    }
  }
}