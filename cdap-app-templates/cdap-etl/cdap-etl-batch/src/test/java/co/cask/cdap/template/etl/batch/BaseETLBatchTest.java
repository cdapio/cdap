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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.batch.sink.BatchCubeSink;
import co.cask.cdap.template.etl.batch.sink.DBSink;
import co.cask.cdap.template.etl.batch.sink.KVTableSink;
import co.cask.cdap.template.etl.batch.sink.TableSink;
import co.cask.cdap.template.etl.batch.sink.TimePartitionedFileSetDatasetAvroSink;
import co.cask.cdap.template.etl.batch.source.DBSource;
import co.cask.cdap.template.etl.batch.source.KVTableSource;
import co.cask.cdap.template.etl.batch.source.StreamBatchSource;
import co.cask.cdap.template.etl.batch.source.TableSource;
import co.cask.cdap.template.etl.common.DBRecord;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.template.etl.transform.ScriptFilterTransform;
import co.cask.cdap.template.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.template.test.sink.MetaKVTableSink;
import co.cask.cdap.template.test.source.MetaKVTableSource;
import co.cask.cdap.test.TestBase;
import com.google.gson.Gson;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.hsqldb.jdbc.JDBCDriver;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Base test class that sets up plugins and the batch template.
 */
public class BaseETLBatchTest extends TestBase {
  protected static final Id.Namespace NAMESPACE = Constants.DEFAULT_NAMESPACE_ID;
  protected static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLBatch");
  protected static final Gson GSON = new Gson();

  @BeforeClass
  public static void setupTest() throws IOException {
    addTemplatePlugins(TEMPLATE_ID, "batch-sources-1.0.0.jar",
      DBSource.class, KVTableSource.class, StreamBatchSource.class, TableSource.class, DBRecord.class);
    addTemplatePlugins(TEMPLATE_ID, "batch-sinks-1.0.0.jar",
      BatchCubeSink.class, DBSink.class, KVTableSink.class, TableSink.class,
      TimePartitionedFileSetDatasetAvroSink.class, AvroKeyOutputFormat.class, AvroKey.class);
    addTemplatePlugins(TEMPLATE_ID, "test-sources-1.0.0.jar", MetaKVTableSource.class);
    addTemplatePlugins(TEMPLATE_ID, "test-sinks-1.0.0.jar", MetaKVTableSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar",
      ProjectionTransform.class, ScriptFilterTransform.class, StructuredRecordToGenericRecordTransform.class);
    addTemplatePlugins(TEMPLATE_ID, "hsql-jdbc-1.0.0.jar", JDBCDriver.class);
    addTemplatePluginJson(TEMPLATE_ID, "hsql-jdbc-1.0.0.json", "jdbc", "hypersql", "hypersql jdbc driver",
      JDBCDriver.class.getName());
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLBatchTemplate.class,
      PipelineConfigurable.class.getPackage().getName(),
      BatchSource.class.getPackage().getName());
  }
}
