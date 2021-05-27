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
package org.apache.spark.sql.hive.io;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

public class MapredOrcInputFormat extends FileInputFormat {

  private static final Log LOG = LogFactory.getLog(MapredOrcInputFormat.class);

  private final OrcInputFormat realInput;

  public MapredOrcInputFormat(){
    this(new OrcInputFormat());
  }

  public MapredOrcInputFormat(OrcInputFormat inputFormat){
    this.realInput = inputFormat;
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    try {
      LOG.info("Current file split: "+ inputSplit.toString());
      return realInput.getRecordReader(inputSplit, jobConf, reporter);
    } catch (Exception e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }
}

