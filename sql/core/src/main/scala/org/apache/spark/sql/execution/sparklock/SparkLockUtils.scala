package org.apache.spark.sql.execution.sparklock

import org.apache.spark.sql.catalyst.catalog.CatalogTable

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
}
