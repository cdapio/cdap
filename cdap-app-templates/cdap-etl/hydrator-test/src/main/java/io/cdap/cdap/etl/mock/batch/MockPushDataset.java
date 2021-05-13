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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.SerializableTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Push Dataset implementation for unit test
 */
public class MockPushDataset implements SQLPushDataset<StructuredRecord, Object, Object>, Serializable {
  private final AtomicLong numRows = new AtomicLong(0L);
  private final SQLPushRequest pushRequest;
  private final String dirName;

  public MockPushDataset(SQLPushRequest pushRequest, String dirName) {
    this.pushRequest = pushRequest;
    File dir = new File(dirName);
    File childDir = new File(dir, pushRequest.getDatasetName());
    this.dirName = childDir.getAbsolutePath();
  }

  @Override
  public String getOutputFormatClassName() {
    return TextOutputFormat.class.getCanonicalName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    Map<String, String> conf = new HashMap<>();
    conf.put(TextOutputFormat.OUTDIR, dirName);

    return ImmutableMap.copyOf(conf);
  }

  @Override
  public Transform<StructuredRecord, KeyValue<Object, Object>> toKeyValue() {
    return new SerializableTransform<StructuredRecord, KeyValue<Object, Object>>() {
      @Override
      public void transform(StructuredRecord input, Emitter<KeyValue<Object, Object>> emitter) throws Exception {
        numRows.incrementAndGet();
        String output = StructuredRecordStringConverter.toJsonString(input);
        emitter.emit(new KeyValue<>(output, output));
      }
    };
  }

  @Override
  public String getDatasetName() {
    return pushRequest.getDatasetName();
  }

  @Override
  public Schema getSchema() {
    return pushRequest.getDatasetSchema();
  }

  @Override
  public long getNumRows() {
    return pushRequest.getDatasetName().equalsIgnoreCase("users") ? 3 : 2;
  }
}
