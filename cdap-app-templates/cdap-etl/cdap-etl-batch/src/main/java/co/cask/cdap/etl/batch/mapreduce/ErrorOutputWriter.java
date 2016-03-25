/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Collection;

/**
 * Writes to error datasets. This could use a decent amount of improvement.
 *
 * The first issue is that this uses avro directly, which means the app requires a specific avro version,
 * and plugins will run into issues if they want to use a different avro (CDAP-3809).
 *
 * The second issue is that it assumes everything is a StructuredRecord and casts blindly.
 *
 * @param <KEY_OUT> output key type
 * @param <VAL_OUT> output value type
 */
class ErrorOutputWriter<KEY_OUT, VAL_OUT> {
  private static final org.apache.avro.Schema AVRO_ERROR_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(Constants.ERROR_SCHEMA.toString());
  private final MapReduceTaskContext<KEY_OUT, VAL_OUT> context;
  private final String errorDatasetName;

  ErrorOutputWriter(MapReduceTaskContext<KEY_OUT, VAL_OUT> context, String errorDatasetName) {
    this.context = context;
    this.errorDatasetName = errorDatasetName;
  }

  void write(Collection<InvalidEntry<Object>> input) throws Exception {
    for (InvalidEntry entry : input) {
      context.write(errorDatasetName, new AvroKey<>(getGenericRecordForInvalidEntry(entry)),
                    NullWritable.get());
    }
  }

  private GenericRecord getGenericRecordForInvalidEntry(InvalidEntry invalidEntry) {
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AVRO_ERROR_SCHEMA);
    recordBuilder.set(Constants.ErrorDataset.ERRCODE, invalidEntry.getErrorCode());
    recordBuilder.set(Constants.ErrorDataset.ERRMSG, invalidEntry.getErrorMsg());

    String errorMsg;
    if (invalidEntry.getInvalidRecord() instanceof StructuredRecord) {
      StructuredRecord record = (StructuredRecord) invalidEntry.getInvalidRecord();
      try {
        errorMsg = StructuredRecordStringConverter.toJsonString(record);
      } catch (IOException e) {
        errorMsg = "Exception while converting StructuredRecord to String, " + e.getCause();
      }
    } else {
      errorMsg = String.format("Error Entry is of type %s, only a record of type %s is supported currently",
                               invalidEntry.getInvalidRecord().getClass().getName(),
                               StructuredRecord.class.getName());
    }
    recordBuilder.set(Constants.ErrorDataset.INVALIDENTRY, errorMsg);
    return recordBuilder.build();
  }
}
