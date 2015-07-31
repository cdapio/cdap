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

package co.cask.cdap.template.etl.realtime;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.template.etl.api.PipelineConfigurable;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.common.ETLStage;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.template.etl.realtime.sink.RealtimeCubeSink;
import co.cask.cdap.template.etl.realtime.sink.RealtimeTableSink;
import co.cask.cdap.template.etl.realtime.sink.StreamSink;
import co.cask.cdap.template.etl.realtime.source.DataGeneratorSource;
import co.cask.cdap.template.etl.realtime.source.JmsSource;
import co.cask.cdap.template.etl.realtime.source.KafkaSource;
import co.cask.cdap.template.etl.realtime.source.SqsSource;
import co.cask.cdap.template.etl.realtime.source.TwitterSource;
import co.cask.cdap.template.etl.transform.ProjectionTransform;
import co.cask.cdap.template.etl.transform.ScriptFilterTransform;
import co.cask.cdap.template.etl.transform.StructuredRecordToGenericRecordTransform;
import co.cask.cdap.test.AdapterManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RealtimeCubeSinkTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final Gson GSON = new Gson();
  private static final Id.Namespace NAMESPACE = Constants.DEFAULT_NAMESPACE_ID;
  private static final Id.ApplicationTemplate TEMPLATE_ID = Id.ApplicationTemplate.from("ETLRealtime");

  @BeforeClass
  public static void setupTests() throws IOException {
    // todo: should only deploy test source and cube sink
    addTemplatePlugins(TEMPLATE_ID, "realtime-sources-1.0.0.jar",
                       DataGeneratorSource.class, JmsSource.class, KafkaSource.class,
                       TwitterSource.class, SqsSource.class);
    addTemplatePlugins(TEMPLATE_ID, "realtime-sinks-1.0.0.jar",
                       RealtimeCubeSink.class, RealtimeTableSink.class, StreamSink.class);
    addTemplatePlugins(TEMPLATE_ID, "transforms-1.0.0.jar",
                       ProjectionTransform.class, ScriptFilterTransform.class,
                       StructuredRecordToGenericRecordTransform.class);
    deployTemplate(NAMESPACE, TEMPLATE_ID, ETLRealtimeTemplate.class,
                   PipelineConfigurable.class.getPackage().getName(),
                   ETLStage.class.getPackage().getName(),
                   RealtimeSource.class.getPackage().getName());
  }

  @Test
  public void test() throws Exception {
    ETLStage source = new ETLStage("DataGenerator", ImmutableMap.of(DataGeneratorSource.PROPERTY_TYPE,
                                                                    DataGeneratorSource.TABLE_TYPE));
    // single aggregation
    Map<String, String> datasetProps = ImmutableMap.of(
      CubeDatasetDefinition.PROPERTY_AGGREGATION_PREFIX + "byName.dimensions", "name"
    );
    Map<String, String> measurementsProps = ImmutableMap.of(
      Properties.Cube.MEASUREMENT_PREFIX + "score", "GAUGE"
    );
    ETLStage sink = new ETLStage("Cube",
                                 ImmutableMap.of(Properties.Cube.DATASET_NAME, "cube1",
                                                 Properties.Cube.DATASET_OTHER, new Gson().toJson(datasetProps),
                                                 Properties.Cube.MEASUREMENTS, new Gson().toJson(measurementsProps)));
    ETLRealtimeConfig etlConfig = new ETLRealtimeConfig(source, sink, Lists.<ETLStage>newArrayList());
    AdapterConfig adapterConfig = new AdapterConfig("", TEMPLATE_ID.getId(), GSON.toJsonTree(etlConfig));
    Id.Adapter adapterId = Id.Adapter.from(NAMESPACE, "testCubeSink");

    AdapterManager manager = createAdapter(adapterId, adapterConfig);

    long startTs = System.currentTimeMillis() / 1000;

    manager.start();
    // Let the worker run for 5 seconds
    TimeUnit.SECONDS.sleep(5);
    manager.stop();

    long endTs = System.currentTimeMillis() / 1000;

    // verify
    DataSetManager<Cube> tableManager = getDataset("cube1");
    Cube cube = tableManager.get();
    Collection<TimeSeries> result = cube.query(CubeQuery.builder()
                                                 .select().measurement("score", AggregationFunction.LATEST)
                                                 .from("byName").resolution(1, TimeUnit.SECONDS)
                                                 .where().timeRange(startTs, endTs).limit(100).build());
    Assert.assertFalse(result.isEmpty());
    Iterator<TimeSeries> iterator = result.iterator();
    Assert.assertTrue(iterator.hasNext());
    TimeSeries timeSeries = iterator.next();
    Assert.assertEquals("score", timeSeries.getMeasureName());
    Assert.assertFalse(timeSeries.getTimeValues().isEmpty());
    Assert.assertEquals(3, timeSeries.getTimeValues().get(0).getValue());
    Assert.assertFalse(iterator.hasNext());
  }
}
