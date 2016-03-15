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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.PipelineConfigurable;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.mapreduce.SumAggregation;
import co.cask.cdap.etl.batch.mock.GroupByBatchAggregation;
import co.cask.cdap.etl.batch.mock.MockSink;
import co.cask.cdap.etl.batch.mock.MockSource;
import co.cask.cdap.etl.batch.mock.StringValueFilterTransform;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.twill.filesystem.Location;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

/**
 * Base test class that sets up plugins and the batch template.
 */
public class ETLBatchTestBase extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "etlbatch", "3.2.0");
  protected static final ArtifactSummary ETLBATCH_ARTIFACT = new ArtifactSummary("etlbatch", "3.2.0");

  private static int startCount;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    // add the artifact for etl batch app
    addAppArtifact(APP_ARTIFACT_ID, ETLBatchApplication.class,
      BatchSource.class.getPackage().getName(),
      PipelineConfigurable.class.getPackage().getName(),
      "org.apache.avro.mapred", "org.apache.avro", "org.apache.avro.generic", "org.apache.avro.io");

    // add some test plugins
    addPluginArtifact(Id.Artifact.from(Id.Namespace.DEFAULT, "test-plugins", "1.0.0"), APP_ARTIFACT_ID,
                      MockSource.class, MockSink.class, StringValueFilterTransform.class,
                      GroupByBatchAggregation.class, SumAggregation.class);
  }

  protected List<GenericRecord> readOutput(TimePartitionedFileSet fileSet, Schema schema) throws IOException {
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema.toString());
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
    List<GenericRecord> records = Lists.newArrayList();
    for (Location dayLoc : fileSet.getEmbeddedFileSet().getBaseLocation().list()) {
      // this level should be the day (ex: 2015-01-19)
      for (Location timeLoc : dayLoc.list()) {
        // this level should be the time (ex: 21-23.1234567890000)
        for (Location file : timeLoc.list()) {
          // this level should be the actual mapred output
          String locName = file.getName();

          if (locName.endsWith(".avro")) {
            DataFileStream<GenericRecord> fileStream = new DataFileStream<>(file.getInputStream(), datumReader);
            Iterables.addAll(records, fileStream);
            fileStream.close();
          }
        }
      }
    }
    return records;
  }
}
