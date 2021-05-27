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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;



import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineAvroContainerInputFormat
    extends CombineFileInputFormat<NullWritable, AvroGenericRecordWritable> {

  @SuppressWarnings({"rawtypes","unchecked"})
  public RecordReader<NullWritable, AvroGenericRecordWritable> getRecordReader(InputSplit split,
                                                                               JobConf jobConf, Reporter reporter) throws IOException {
    return new CombineFileRecordReader(jobConf,(CombineFileSplit)split, reporter, AvroContainerRecordReaderWrapper.class);
  }

  private static class AvroContainerRecordReaderWrapper extends CombineFileRecordReaderWrapper<NullWritable, AvroGenericRecordWritable> {
    public AvroContainerRecordReaderWrapper(CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx) throws IOException {
      super(new AvroContainerInputFormat(), split, conf, reporter, idx);
    }
  }
}
