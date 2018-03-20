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

package co.cask.cdap.report;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
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
    Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("runtimeArguments", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)))
    );

  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
    Schema.Field.of("namespace", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("application", Schema.of(Schema.Type.STRING)),
//    Schema.Field.of("version", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("program", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("run", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("status", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("time", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("startInfo",  Schema.nullableOf(STARTING_INFO))
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

//  public static Set<GenericRecord> readOutput(Location location) throws IOException {
//    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(SCHEMA);
//    Set<GenericRecord> records = new HashSet<>();
//    for (Location file : location.list()) {
//      if (file.getName().endsWith(".avro")) {
//        DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
//        Iterables.addAll(records, fileStream);
//        fileStream.close();
//      }
//    }
//    return records;
//  }

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
