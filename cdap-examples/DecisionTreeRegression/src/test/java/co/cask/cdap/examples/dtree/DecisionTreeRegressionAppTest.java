/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.examples.dtree;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.io.InputSupplier;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Decision Tree Example tests.
 */
public class DecisionTreeRegressionAppTest extends TestBaseWithSpark2 {
  private static final Gson GSON = new Gson();

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void test() throws Exception {
    // Deploy the Application
    ApplicationManager appManager = deployApplication(DecisionTreeRegressionApp.class);

    // Start the Service
    ServiceManager serviceManager = appManager.getServiceManager(ModelDataService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true, 30, 1);

    URL serviceURL = serviceManager.getServiceURL(15, TimeUnit.SECONDS);
    URL addDataURL = new URL(serviceURL, "labels");
    HttpRequest request = HttpRequest.builder(HttpMethod.PUT, addDataURL)
      .withBody(new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return getClass().getClassLoader().getResourceAsStream("sample_libsvm_data.txt");
        }
      })
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    // Start a Spark Program
    SparkManager sparkManager = appManager.getSparkManager(ModelTrainer.NAME).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // Check that there is a new model
    URL listModelsURL = new URL(serviceURL, "models");
    request = HttpRequest.builder(HttpMethod.GET, listModelsURL).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    List<String> models = GSON.fromJson(response.getResponseBodyAsString(),
                                        new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(1, models.size());

    // Check that there is some model metadata
    String modelId = models.get(0);
    URL modelMetaURL = new URL(serviceURL, "models/" + modelId);
    request = HttpRequest.builder(HttpMethod.GET, modelMetaURL).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    ModelMeta meta = GSON.fromJson(response.getResponseBodyAsString(), ModelMeta.class);
    Assert.assertNotNull(meta);
    Assert.assertEquals(0.7, meta.getTrainingPercentage(), 0.000001);
    Assert.assertEquals(692, meta.getNumFeatures());
    // Check that the corresponding model file exists
    DataSetManager<FileSet> modelFiles = getDataset(DecisionTreeRegressionApp.MODEL_DATASET);
    Assert.assertTrue(modelFiles.get().getBaseLocation().append(modelId).exists());
  }
}
