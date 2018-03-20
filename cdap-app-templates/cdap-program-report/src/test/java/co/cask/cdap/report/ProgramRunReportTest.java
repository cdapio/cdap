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

import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ProgramRunReportTest extends TestBaseWithSpark2 {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunReportTest.class);
  private static final Gson GSON = new Gson();

  // TODO: Temporarily disable this test because of problems with running the test
  //  @Test
  public void testGenerateReport() throws Exception {
    ApplicationManager app = deployApplication(ReportGenerationApp.class);
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start();
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports").toURL();
    List<ReportGenerationRequest.Filter> filters =
      ImmutableList.of(
        new ReportGenerationRequest.ValueFilter<>("namespace", ImmutableList.of("ns1", "ns2"), null),
        new ReportGenerationRequest.RangeFilter<>("duration", new ReportGenerationRequest.Range<>(null, 30L)));
    ReportGenerationRequest request =
      new ReportGenerationRequest(1520808000L, 1520808301L, ImmutableList.of("namespace", "duration", "user"),
                                  ImmutableList.of(
                                    new ReportGenerationRequest.Sort("duration",
                                                                     ReportGenerationRequest.Order.DESCENDING)),
                                  filters);
    HttpURLConnection urlConn = (HttpURLConnection) reportURL.openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestMethod("POST");
    urlConn.getOutputStream().write(GSON.toJson(request).getBytes(StandardCharsets.UTF_8));
    int responseCode = urlConn.getResponseCode();
    Assert.assertEquals(200, responseCode);
    String msg = urlConn.getResponseMessage();
  }
}
