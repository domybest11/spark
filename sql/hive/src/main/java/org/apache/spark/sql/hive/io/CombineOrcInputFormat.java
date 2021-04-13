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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.JobContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineOrcInputFormat
    extends CombineFileInputFormat<NullWritable, OrcStruct> {

  private static final Log LOG = LogFactory.getLog(CombineOrcInputFormat.class);

  @SuppressWarnings({"rawtypes","unchecked"})
  public RecordReader<NullWritable, OrcStruct> getRecordReader(InputSplit split,
                                                               JobConf jobConf, Reporter reporter) throws IOException {
    LOG.info("Combine file splits: "+ split.toString());
    return new CombineFileRecordReader(jobConf,(CombineFileSplit)split, reporter,
        OrcStructRecordReaderWrapper.class);
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = super.listStatus(job);
    List<FileStatus> ok = new ArrayList<FileStatus>(result.size());
    for (FileStatus stat : result) {
      if (stat.getLen() != 0) {
        ok.add(stat);
      } else {
        LOG.info("Filtering out the 0 byte files : " + stat.getPath());
      }
    }
    if (ok.size() == result.size()) {
      return result;
    } else {
      return ok;
    }
  }


    private static class OrcStructRecordReaderWrapper extends CombineFileRecordReaderWrapper<NullWritable, OrcStruct> {
    public OrcStructRecordReaderWrapper(CombineFileSplit split, Configuration conf,
                                        Reporter reporter, Integer idx) throws IOException {
      super(new MapredOrcInputFormat(), split, conf, reporter, idx);
    }
  }
}
