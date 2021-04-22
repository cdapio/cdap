/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.batch;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pull Dataset implementation for unit test
 */
public class MockPullDataset implements SQLPullDataset<StructuredRecord, Object, Object>, Serializable {
  private static final Gson GSON = new Gson();
  private final AtomicLong numRows = new AtomicLong(0L);
  private final SQLPullRequest pullRequest;
  private final String dirName;

  public MockPullDataset(SQLPullRequest pullRequest, String dirName) {
    this.pullRequest = pullRequest;
    this.dirName = dirName;
  }

  @Override
  public String getInputFormatClassName() {
    return TextInputFormat.class.getCanonicalName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return ImmutableMap.of(TextInputFormat.INPUT_DIR, dirName);
  }

  @Override
  public Transform<KeyValue<Object, Object>, StructuredRecord> fromKeyValue() {
    return new SerializableTransform<KeyValue<Object, Object>, StructuredRecord>() {
      @Override
      public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
        numRows.incrementAndGet();
        emitter.emit(StructuredRecordStringConverter.fromJsonString(input.getValue().toString(), getSchema()));
      }
    };
  }

  @Override
  public String getDatasetName() {
    return pullRequest.getDatasetName();
  }

  @Override
  public Schema getSchema() {
    return pullRequest.getDatasetSchema();
  }

  @Override
  public long getNumRows() {
    return numRows.get();
  }
}
