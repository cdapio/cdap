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

package co.cask.cdap.internal.app.report;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Iterables;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Reads program run meta files.
 */
public class ProgramRunMetaFileReader {
  private static final Schema STARTING_INFO = Schema.recordOf(
    "ProgramStartingInfo",
    Schema.Field.of("artifactName", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("artifactVersion", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("artifactScope", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("runtimeArguments", Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING))),

    )

  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
    Schema.Field.of("application", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("version", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("program", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("run", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("time", Schema.of(Schema.Type.LONG)),

    Schema.Field.of("programStatusInfo", Schema.recordOf(Schema.Type.INT))).toString();

  static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);

  private Set<GenericRecord> readOutput(Location location) throws IOException {
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

  private static GenericData.Record createRecord(String name, int zip) {
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put("name", name);
    record.put("zip", zip);
    return record;
  }
}
