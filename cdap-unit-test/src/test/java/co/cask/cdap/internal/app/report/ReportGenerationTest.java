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

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Test {@link ReportGenerationSpark}
 */
public class ReportGenerationTest extends AppFabricTestBase {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  public static String outputFolder;

  private static String metaFile;

  @BeforeClass
  public static void init() throws Exception {
    outputFolder = TEMP_FOLDER.newFolder().getAbsolutePath();
    File tmpFile = File.createTempFile("ProgramRunMeta", ".avro", TEMP_FOLDER.newFolder());
    metaFile = tmpFile.getAbsolutePath();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(ProgramRunMetaFileUtil.SCHEMA);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(ProgramRunMetaFileUtil.SCHEMA, tmpFile);
    String program = "SmartWorkflow";
    String run = "randomRunId";
    long time = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.DAYS.toSeconds(1);
    long delay = TimeUnit.MINUTES.toSeconds(5);
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, ProgramRunStatus.STARTING, time,
                                                              ProgramRunMetaFileUtil.startingInfo("user")));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program, run, ProgramRunStatus.RUNNING,
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, ProgramRunStatus.STARTING,
                                                              time + delay, null));
    dataFileWriter.append(ProgramRunMetaFileUtil.createRecord(program + "_1", run, ProgramRunStatus.RUNNING,
                                                              time + 2 * delay, null));
    dataFileWriter.close();
  }

  @Test
  public void testReportGeneration() throws Exception {
    deploy(ProgramOperationReportApp.class);
    ProgramId reportSpark = new ProgramId(NamespaceId.DEFAULT.getNamespace(), "ProgramOperationReportApp",
                                          ProgramType.SPARK, "ReportGenerationSpark");
    startProgram(reportSpark, ImmutableMap.of("input", metaFile, "output",
                                              TEMP_FOLDER.newFolder().getAbsolutePath()), 200);

  }
}
