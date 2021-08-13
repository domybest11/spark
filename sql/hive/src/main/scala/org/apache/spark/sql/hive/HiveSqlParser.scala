/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.parser.ParserUtils.{operationNotAllowed, withOrigin}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.internal.VariableSubstitution

class HiveSqlParser extends AbstractSqlParser {
  val astBuilder = new HiveSqlAstBuilder()

  private val substitutor = new VariableSubstitution()

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

}

class HiveSqlAstBuilder extends SparkSqlAstBuilder {
  /**
   * Create a [[TableIdentifier]]
   * from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
  override def visitTableIdentifier(
      ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    ctx.table.getText.split("\\.").toSeq match {
      case Seq(tableName: String) =>
        TableIdentifier(tableName, Option(ctx.db).map(_.getText))
      case Seq(database: String, tableName: String) =>
        TableIdentifier(tableName, Option(database))
      case _ => operationNotAllowed(s"`${ctx.table.getText}` does not support as table name", ctx)
    }
  }

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).flatMap( t => t.split("\\.")).toSeq
    }


  /**
   * Validate a create table statement and return the [[TableIdentifier]].
   */
  override def visitCreateTableHeader(
      ctx: CreateTableHeaderContext): TableHeader = withOrigin(ctx) {
    val temporary = ctx.TEMPORARY != null
    val ifNotExists = ctx.EXISTS != null
    if (temporary && ifNotExists) {
      operationNotAllowed("CREATE TEMPORARY TABLE ... IF NOT EXISTS", ctx)
    }
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText).flatMap(
      t => t.split("\\.")
    ).toSeq
    (multipartIdentifier, temporary, ifNotExists, ctx.EXTERNAL != null)
  }

  override def visitReplaceTableHeader(
      ctx: ReplaceTableHeaderContext): TableHeader = withOrigin(ctx) {
    val multipartIdentifier = ctx.multipartIdentifier.parts.asScala.map(_.getText).flatMap(
      t => t.split("\\.")
    ).toSeq
    (multipartIdentifier, false, false, false)
  }

}