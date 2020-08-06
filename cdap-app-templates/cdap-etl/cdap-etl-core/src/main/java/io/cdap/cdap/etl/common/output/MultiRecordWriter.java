/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.etl.common.output;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.common.Constants;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Delegates to other record writers.
 */
public class MultiRecordWriter extends RecordWriter<String, KeyValue<Object, Object>> {
  private final Map<String, List<RecordWriter<Object, Object>>> delegates;

  /**
   * The delegates is a map from sink name to all the record writers for the sink.
   * A sink can contain more than one record writer if it called addOutput() more than once in prepareRun().
   */
  public MultiRecordWriter(Map<String, List<RecordWriter<Object, Object>>> delegates) {
    this.delegates = delegates;
  }

  @Override
  public void write(String key, KeyValue<Object, Object> kv) throws IOException, InterruptedException {
    Collection<RecordWriter<Object, Object>> sinkDelegates = delegates.get(key);
    if (sinkDelegates == null) {
      // this means there was a bug, every sink output should be represented in the delegates map.
      throw new IOException(String.format(
        "Unable to find a writer for output '%s'. This means there is a planner error. " +
          "Please report this bug and turn off stage consolidation by setting '%s' to 'false' in the" +
          "runtime arguments for the pipeline.", key, Constants.CONSOLIDATE_STAGES));
    }
    for (RecordWriter<Object, Object> delegate : sinkDelegates) {
      delegate.write(kv.getKey(), kv.getValue());
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    IOException ex = null;
    for (Collection<RecordWriter<Object, Object>> delegateList : delegates.values()) {
      for (RecordWriter<Object, Object> delegate : delegateList) {
        try {
          delegate.close(context);
        } catch (IOException e) {
          if (ex == null) {
            ex = e;
          } else {
            ex.addSuppressed(e);
          }
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}
