/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.transforms;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.common.DBRecord;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Transforms a record from a database table into a byte array containing the json representation of the record
 */
public class DBRecordToStructuredRecordTransform
  extends Transform<LongWritable, DBRecord, LongWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DBRecordToStructuredRecordTransform.class);

  @Override
  public void transform(@Nullable LongWritable inputKey, DBRecord dbRecord,
                        Emitter<LongWritable, StructuredRecord> emitter) throws Exception {
    if (inputKey == null) {
      LOG.debug("Found null input key. Ignoring.");
      return;
    }
    emitter.emit(inputKey, dbRecord.getRecord());
  }
}
