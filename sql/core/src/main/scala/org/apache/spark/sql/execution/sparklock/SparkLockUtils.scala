package org.apache.spark.sql.execution.sparklock

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SessionState

import java.util.{Date, UUID}
import scala.util.{Failure, Success, Try}

object SparkLockUtils {

  import java.text.SimpleDateFormat

  val sdf = new SimpleDateFormat
  sdf.applyPattern("yyyyMMddHHmmss")

  def tableShouldLock(table: CatalogTable): Boolean ={
    table.properties.getOrElse("lock.tag", false).asInstanceOf[Boolean]
  }

  def getFieldVal(o: Any, name: String): Any = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def makeQueryId: String ={
    val userid = System.getProperty("user.name")
    userid + "_" + sdf.format(new Date) + "_" + UUID.randomUUID.toString
  }

  def buildHiveConf(sparkConf: SparkConf): HiveConf = {
    val hiveConf = new HiveConf(classOf[SessionState])
    sparkConf.getAll.toMap.foreach {
      case (k, v) => hiveConf.set(k, v)
    }
    hiveConf
  }
}
