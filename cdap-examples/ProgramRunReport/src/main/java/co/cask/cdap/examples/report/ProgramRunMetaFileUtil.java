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

package co.cask.cdap.examples.report;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Iterables;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Utility class for reading and writing program run meta files.
 */
public class ProgramRunMetaFileUtil {
  private static final Schema STARTING_INFO = Schema.recordOf(
    "ProgramStartingInfo",
//    Schema.Field.of("artifactName", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("artifactVersion", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("artifactScope", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("user", Schema.of(Schema.Type.STRING))
//    Schema.Field.of("runtimeArguments", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
//    Schema.Field.of("application", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("version", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("program", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("run", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("status", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("time", Schema.of(Schema.Type.LONG))
//    Schema.Field.of("statusInfo",  Schema.nullableOf(STARTING_INFO))
  ).toString();

  public static final String RUN_META_FILE = "/Users/Chengfeng/tmp/run_meta.avro";
  public static final org.apache.avro.Schema STARTING_INFO_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(STARTING_INFO.toString());
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);

  private static final class ProgramStartingInfo {
    private final String user;

    ProgramStartingInfo(String user) {
      this.user = user;
    }

    public String getUser() {
      return user;
    }
  }

  public static ProgramStartingInfo startingInfo(String user) {
    return new ProgramStartingInfo(user);
  }

  public static Set<GenericRecord> readOutput(Location location) throws IOException {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SCHEMA);
    Set<GenericRecord> records = new HashSet<>();
    for (Location file : location.list()) {
      if (file.getName().endsWith(".avro")) {
        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
        Iterables.addAll(records, fileStream);
        fileStream.close();
      }
    }
    return records;
  }

  public static GenericData.Record createRecord(String program, String run, String status, long time,
                                                @Nullable ProgramStartingInfo statusInfo) {
    GenericData.Record statusInfoRecord = null;
    if (statusInfo != null) {
      statusInfoRecord = new GenericData.Record(STARTING_INFO_SCHEMA);
      statusInfoRecord.put("user", statusInfo.getUser());
    }
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("program", program);
    record.put("run", run);
    record.put("status", status);
    record.put("time", time);
//    record.put("statusInfo", statusInfoRecord);
    return record;
  }

  public static void main(String[] args) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    File tmpFile = new File("/Users/Chengfeng/tmp/run_meta.avro");
    dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, tmpFile);
    String program = "SmartWorkflow";
    String run = "randomRunId";
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.DAYS.toSeconds(1);
    long delay = TimeUnit.MINUTES.toSeconds(5);
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, "STARTING", time,
                                                              ProgramRunMetaFileUtil.startingInfo("user")));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, "RUNNING",
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, "STARTING",
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, "RUNNING",
                                                              time + 2 * delay, null));
    dataFileWriter.close();
  }
}
