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

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.report.util.Constants;
import org.apache.avro.generic.GenericData;

/**
 * serializer for {@link ProgramRunIdFields}
 */
public class ProgramRunIdFieldsSerializer {

  private static final Schema ARTIFACT_INFO = Schema.recordOf(
    "artifactInfo",
    Schema.Field.of(Constants.ARTIFACT_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.ARTIFACT_SCOPE, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.ARTIFACT_VERSION, Schema.of(Schema.Type.STRING)));

  private static final Schema STARTING_INFO = Schema.recordOf(
    "ProgramStartingInfo",
    Schema.Field.of(Constants.USER, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of(Constants.RUNTIME_ARGUMENTS, Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                              Schema.of(Schema.Type.STRING))
    ),
    Schema.Field.of(Constants.ARTIFACT_ID, ARTIFACT_INFO));


  private static final String SCHEMA_STRING = Schema.recordOf(
    "ReportRecord",
    Schema.Field.of(Constants.NAMESPACE, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.PROGRAM, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.RUN, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.STATUS, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.TIME, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(Constants.MESSAGE_ID, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(Constants.START_INFO, Schema.nullableOf(STARTING_INFO))
  ).toString();

  public static final org.apache.avro.Schema ARTIFACT_INFO_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(ARTIFACT_INFO.toString());
  public static final org.apache.avro.Schema STARTING_INFO_SCHEMA =
    new org.apache.avro.Schema.Parser().parse(STARTING_INFO.toString());
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(SCHEMA_STRING);


  public static GenericData.Record createRecord(ProgramRunIdFields runIdFields) {
    GenericData.Record startInfoRecord = null;
    if (runIdFields.getProgramStatus().equals("STARTING")) {
      startInfoRecord = new GenericData.Record(STARTING_INFO_SCHEMA);
      startInfoRecord.put(Constants.USER, runIdFields.getProgramSartInfo().getPrincipal());
      startInfoRecord.put(Constants.RUNTIME_ARGUMENTS, runIdFields.getProgramSartInfo().getRuntimeArguments());
      GenericData.Record artifactRecord = new GenericData.Record(ARTIFACT_INFO_SCHEMA);
      ArtifactId artifactId = runIdFields.getProgramSartInfo().getArtifactId();
      artifactRecord.put(Constants.ARTIFACT_NAME, artifactId.getName());
      artifactRecord.put(Constants.ARTIFACT_VERSION, artifactId.getVersion().toString());
      artifactRecord.put(Constants.ARTIFACT_SCOPE, artifactId.getScope().toString());
      startInfoRecord.put(Constants.ARTIFACT_ID, artifactRecord);
    }
    GenericData.Record record = new GenericData.Record(SCHEMA);
    record.put(Constants.NAMESPACE, runIdFields.getNamespace());
    record.put(Constants.PROGRAM, runIdFields.getProgram());
    record.put(Constants.RUN, runIdFields.getRun());
    record.put(Constants.STATUS, runIdFields.getProgramStatus());
    record.put(Constants.TIME, runIdFields.getTimestamp());
    record.put(Constants.MESSAGE_ID, runIdFields.getMessageId());
    record.put(Constants.START_INFO, startInfoRecord);
    return record;
  }
}
