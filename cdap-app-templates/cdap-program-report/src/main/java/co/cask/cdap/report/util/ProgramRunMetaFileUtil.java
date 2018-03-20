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
public class ProgramRunMetaFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunMetaFileUtil.class);

  private static final Schema STARTING_INFO = Schema.recordOf(
    "ProgramStartingInfo",
//    Schema.Field.of("artifactName", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("artifactVersion", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("artifactScope", Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.USER, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.RUNTIME_ARGUMENTS, Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
    Schema.Field.of(Constants.NAMESPACE, Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("application", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("version", Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.PROGRAM, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.RUN, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.STATUS, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.TIME, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(Constants.START_INFO,  Schema.nullableOf(STARTING_INFO))
  ).toString();

  public static final String RUN_META_FILE = "/Users/Chengfeng/tmp/run_meta.avro";
  public static final org.apache.avro.Schema STARTING_INFO_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(STARTING_INFO.toString());
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);

  private static final class ProgramStartInfo {
    private final String user;

    ProgramStartInfo(String user) {
      this.user = user;
    }

    public String getUser() {
      return user;
    }
  }

  public static ProgramStartInfo startingInfo(String user) {
    return new ProgramStartInfo(user);
  }

  public static void populateMetaFiles(Location metaBaseLocation) throws Exception {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    for (String namespace : ImmutableList.of("default", "ns1", "ns2")) {
      Location nsLocation = metaBaseLocation.append(namespace);
      nsLocation.mkdirs();
      for (int i = 0; i < 5; i++) {
        long time = 1520808000L + 1000 * i;
        Location reportLocation = nsLocation.append(String.format("%d.avro", time));
        reportLocation.createNew();
        dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, reportLocation.getOutputStream());
        String program = "SmartWorkflow";
        String run1 = ReportIds.generate().toString();
        String run2 = ReportIds.generate().toString();
        long delay = TimeUnit.MINUTES.toSeconds(5);
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program, run1, "STARTING",
                                                                  time, ProgramRunMetaFileUtil.startingInfo("user")));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program, run1,
                                                                  "FAILED", time + delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "STARTING", time + delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "RUNNING", time + 2 * delay, null));
        dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(namespace, program + "_1", run2,
                                                                  "COMPLETED", time + 4 * delay, null));
        dataFileWriter.close();
      }
      LOG.debug("nsLocation.list() = {}", nsLocation.list());
    }
  }

  public static GenericData.Record createRecord(String namespace, String program, String run, String status, long time,
                                                @Nullable ProgramStartInfo startInfo) {
    GenericData.Record startInfoRecord = null;
    if (startInfo != null) {
      startInfoRecord = new GenericData.Record(STARTING_INFO_SCHEMA);
      startInfoRecord.put("user", startInfo.getUser());
      startInfoRecord.put("runtimeArguments", ImmutableMap.of("k1", "v1", "k2", "v2"));
    }
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("namespace", namespace);
    record.put("program", program);
    record.put("run", run);
    record.put("status", status);
    record.put("time", time);
    record.put("startInfo", startInfoRecord);
    return record;
  }
}
