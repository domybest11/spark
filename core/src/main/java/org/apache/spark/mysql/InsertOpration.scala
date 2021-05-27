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

package org.apache.spark.mysql

object InsertOpration {

  def apply(stype: String, etlRespository: CallChain.EtlRespository)
  = new OprationImpl().apply(etlRespository, stype)

  def apply(stype: String, adhocRespository: CallChain.AdhocRespository)
  = new OprationImpl().apply(adhocRespository, stype)

  def apply(content: String, adhocRespository: CallChain.AdhocRespository, line: Boolean)
  = new OprationImpl().apply(adhocRespository, content, true)

  def apply(content: String, etlRespository: CallChain.EtlRespository, line: Boolean)
  = new OprationImpl().apply(etlRespository, content, true)

}
