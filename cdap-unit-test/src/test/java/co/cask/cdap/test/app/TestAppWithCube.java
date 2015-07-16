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

package co.cask.cdap.test.app;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.CubeExploreQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestAppWithCube extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Gson GSON = new Gson();

  @Category(SlowTests.class)
  @Test
  public void testApp() throws Exception {
    // Deploy the application
    ApplicationManager appManager = deployApplication(AppWithCube.class);

    ServiceManager serviceManager = appManager.getServiceManager(AppWithCube.SERVICE_NAME).start();
    try {
      serviceManager.waitForStatus(true);
      URL url = serviceManager.getServiceURL();

      long tsInSec = System.currentTimeMillis() / 1000;

      // round to a minute for testing minute resolution
      tsInSec = (tsInSec / 60) * 60;

      // add couple facts
      add(url, ImmutableList.of(new CubeFact(tsInSec)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1)));

      add(url, ImmutableList.of(new CubeFact(tsInSec)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1),
                                new CubeFact(tsInSec + 1)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "back")
                                  .addMeasurement("count", MeasureType.COUNTER, 1),
                                new CubeFact(tsInSec + 2)
                                  .addDimensionValue("user", "alex").addDimensionValue("action", "click")
                                  .addMeasurement("count", MeasureType.COUNTER, 1)));

      // search for tags
      Collection<DimensionValue> tags =
        searchDimensionValue(url, new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100,
                                                       new ArrayList<DimensionValue>()));
      Assert.assertEquals(1, tags.size());
      DimensionValue tv = tags.iterator().next();
      Assert.assertEquals("user", tv.getName());
      Assert.assertEquals("alex", tv.getValue());

      tags = searchDimensionValue(url,
                                  CubeExploreQuery.builder()
                                    .from()
                                      .resolution(1, TimeUnit.SECONDS)
                                    .where()
                                      .dimension("user", "alex")
                                      .timeRange(tsInSec - 60, tsInSec + 60)
                                    .limit(100)
                                    .build());
      Assert.assertEquals(2, tags.size());
      Iterator<DimensionValue> iterator = tags.iterator();
      tv = iterator.next();
      Assert.assertEquals("action", tv.getName());
      Assert.assertEquals("back", tv.getValue());
      tv = iterator.next();
      Assert.assertEquals("action", tv.getName());
      Assert.assertEquals("click", tv.getValue());

      // search for measures
      Collection<String> measures =
        searchMeasure(url, new CubeExploreQuery(tsInSec - 60, tsInSec + 60, 1, 100,
                                                ImmutableList.of(new DimensionValue("user", "alex"))));
      Assert.assertEquals(1, measures.size());
      String measure = measures.iterator().next();
      Assert.assertEquals("count", measure);

      // query for data

      // 1-sec resolution
      Collection<TimeSeries> data =
        query(url,
              CubeQuery.builder()
                .select()
                  .measurement("count", AggregationFunction.SUM)
                .from(null)
                  .resolution(1, TimeUnit.SECONDS)
                .where()
                  .dimension("action", "click")
                  .timeRange(tsInSec - 60, tsInSec + 60)
                .limit(100)
                .build());
      Assert.assertEquals(1, data.size());
      TimeSeries series = data.iterator().next();
      List<TimeValue> timeValues = series.getTimeValues();
      Assert.assertEquals(2, timeValues.size());
      TimeValue timeValue = timeValues.get(0);
      Assert.assertEquals(tsInSec, timeValue.getTimestamp());
      Assert.assertEquals(2, timeValue.getValue());
      timeValue = timeValues.get(1);
      Assert.assertEquals(tsInSec + 2, timeValue.getTimestamp());
      Assert.assertEquals(1, timeValue.getValue());

      // 60-sec resolution
      data = query(url, new CubeQuery(null, tsInSec - 60, tsInSec + 60, 60, 100,
                                      ImmutableMap.of("count", AggregationFunction.SUM),
                                      ImmutableMap.of("action", "click"), new ArrayList<String>(), null));
      Assert.assertEquals(1, data.size());
      series = data.iterator().next();
      timeValues = series.getTimeValues();
      Assert.assertEquals(1, timeValues.size());
      timeValue = timeValues.get(0);
      Assert.assertEquals(tsInSec, timeValue.getTimestamp());
      Assert.assertEquals(3, timeValue.getValue());

    } finally {
      serviceManager.stop();
      serviceManager.waitForStatus(false);
    }
  }

  private void add(URL serviceUrl, Collection<CubeFact> facts) throws IOException {
    URL url = new URL(serviceUrl, "add");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(facts)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private Collection<DimensionValue> searchDimensionValue(URL serviceUrl, CubeExploreQuery query) throws IOException {
    URL url = new URL(serviceUrl, "searchDimensionValue");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<DimensionValue>>() { }.getType());
  }

  private Collection<String> searchMeasure(URL serviceUrl, CubeExploreQuery query) throws IOException {
    URL url = new URL(serviceUrl, "searchMeasure");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<String>>() { }.getType());
  }

  private Collection<TimeSeries> query(URL serviceUrl, CubeQuery query) throws IOException {
    URL url = new URL(serviceUrl, "query");
    HttpRequest request = HttpRequest.post(url).withBody(GSON.toJson(query)).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<Collection<TimeSeries>>() { }.getType());
  }
}
