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

package co.cask.cdap.etl.realtime;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.realtime.config.ETLRealtimeConfig;
import co.cask.cdap.etl.realtime.source.DataGeneratorSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RealtimeCubeSinkTest extends ETLRealtimeBaseTest {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

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

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "testCubeSink");
    AppRequest<ETLRealtimeConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkerManager workerManager = appManager.getWorkerManager(ETLWorker.NAME);

    final long startTs = System.currentTimeMillis() / 1000;

    workerManager.start();
    final DataSetManager<Cube> tableManager = getDataset("cube1");
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        tableManager.flush();
        Cube cube = tableManager.get();
        Collection<TimeSeries> result = cube.query(buildCubeQuery(startTs));
        return !result.isEmpty();
      }
    }, 10, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
    workerManager.stop();

    // verify
    Cube cube = tableManager.get();
    Collection<TimeSeries> result = cube.query(buildCubeQuery(startTs));

    Iterator<TimeSeries> iterator = result.iterator();
    Assert.assertTrue(iterator.hasNext());
    TimeSeries timeSeries = iterator.next();
    Assert.assertEquals("score", timeSeries.getMeasureName());
    Assert.assertFalse(timeSeries.getTimeValues().isEmpty());
    Assert.assertEquals(3, timeSeries.getTimeValues().get(0).getValue());
    Assert.assertFalse(iterator.hasNext());
  }

  private CubeQuery buildCubeQuery(long startTs) {
    long endTs = System.currentTimeMillis() / 1000;
    return CubeQuery.builder()
      .select().measurement("score", AggregationFunction.LATEST)
      .from("byName").resolution(1, TimeUnit.SECONDS)
      .where().timeRange(startTs, endTs).limit(100).build();
  }
}
