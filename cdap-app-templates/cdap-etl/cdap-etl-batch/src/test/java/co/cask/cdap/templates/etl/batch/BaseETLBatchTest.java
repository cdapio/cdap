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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.etl.api.EndPointStage;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.batch.sinks.BatchCubeSink;
import co.cask.cdap.templates.etl.batch.sinks.DBSink;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sinks.TableSink;
import co.cask.cdap.templates.etl.batch.sinks.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.templates.etl.batch.sources.DBSource;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.batch.sources.StreamBatchSource;
import co.cask.cdap.templates.etl.batch.sources.TableSource;
import co.cask.cdap.templates.etl.transforms.IdentityTransform;
import co.cask.cdap.templates.etl.transforms.ProjectionTransform;
import co.cask.cdap.templates.etl.transforms.ScriptFilterTransform;
import co.cask.cdap.templates.etl.transforms.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.TestBase;
import com.google.gson.Gson;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Base test class that sets up plugins and the batch template.
 */
public class BaseETLBatchTest extends TestBase {
  protected static final Id.Namespace NAMESPACE = Id.Namespace.from("default");
  protected static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("etlBatch");
  protected static final Gson GSON = new Gson();

  @BeforeClass
  public static void setupTest() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "batch-sources-1.0.0.jar",
      DBSource.class, KVTableSource.class, StreamBatchSource.class, TableSource.class);
    addTemplatePlugins(TEMPLATE_ID, "batch-sinks-1.0.0.jar",
      BatchCubeSink.class, DBSink.class, KVTableSink.class,
      TableSink.class, TimePartitionedFileSetDatasetAvroSink.class, AvroKeyOutputFormat.class, AvroKey.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar", IdentityTransform.class,
      ProjectionTransform.class, ScriptFilterTransform.class, StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLBatchTemplate.class,
      EndPointStage.class.getPackage().getName(),
      BatchSource.class.getPackage().getName());
  }
}
