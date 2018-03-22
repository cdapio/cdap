/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.util;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.report.StartInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Utility class for reading and writing program run meta files.
 */
public final class ProgramRunMetaFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunMetaFileUtil.class);

  // TODO: [CDAP-13215] add omitted fields when the actual program meta files are generated
  private static final Schema STARTING_INFO = Schema.recordOf(
    "ProgramStartingInfo",
    Schema.Field.of(Constants.USER, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.RUNTIME_ARGUMENTS, Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                              Schema.of(Schema.Type.STRING)))
    );

  // TODO: [CDAP-13215] add omitted fields when the actual program meta files are generated
  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
    Schema.Field.of(Constants.NAMESPACE, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.PROGRAM, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.RUN, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.STATUS, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.TIME, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(Constants.START_INFO,  Schema.nullableOf(STARTING_INFO))
  ).toString();

  public static final org.apache.avro.Schema STARTING_INFO_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(STARTING_INFO.toString());
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);

  private ProgramRunMetaFileUtil() {
    // no-op
  }

  /**
   * Creates a mock report record with the given information
   *
   * @return a report record containing the given information
   */
  public static GenericData.Record createRecord(String namespace, String program, String run, String status, long time,
                                                @Nullable StartInfo startInfo) {
    GenericData.Record startInfoRecord = null;
    if (startInfo != null) {
      startInfoRecord = new GenericData.Record(STARTING_INFO_SCHEMA);
      startInfoRecord.put(Constants.USER, startInfo.user());
      startInfoRecord.put(Constants.RUNTIME_ARGUMENTS, startInfo.getRuntimeArgsAsJavaMap());
    }
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put(Constants.NAMESPACE, namespace);
    record.put(Constants.PROGRAM, program);
    record.put(Constants.RUN, run);
    record.put(Constants.STATUS, status);
    record.put(Constants.TIME, time);
    record.put(Constants.START_INFO, startInfoRecord);
    return record;
  }
}
