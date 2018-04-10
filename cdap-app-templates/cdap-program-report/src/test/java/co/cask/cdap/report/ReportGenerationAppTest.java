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
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.report.proto.ReportGenerationRequest;
import co.cask.cdap.report.util.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests {@link ReportGenerationApp}
 */
public class ReportGenerationAppTest extends TestBaseWithSpark2 {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Logger LOG = LoggerFactory.getLogger(ReportGenerationAppTest.class);
  private static final Gson GSON = new Gson();

  @Test
  public void testSimple() throws Exception {
    org.apache.avro.Schema schema = ProgramRunIdFieldsSerializer.SCHEMA;
    System.out.println(schema);
    ProgramRunIdFields runIdFields = new ProgramRunIdFields("app1", "v1", "spark", "prog1", "run1", "ns1");
    runIdFields.setMessageId("ms1");
    runIdFields.setTime(System.currentTimeMillis());
    runIdFields.setStatus("RUNNING");
//    runIdFields.setStartInfo(new ProgramStartInfo(new HashMap<String, String>(),
//                                                  new ArtifactId("a1", new ArtifactVersion("1.0"),
//                                                                 ArtifactScope.SYSTEM), null));
    System.out.println(ProgramRunIdFieldsSerializer.createRecord(runIdFields));
  }
  // TODO: Temporarily ignore this test because of problems with running the test
  @Ignore
  @Test
  public void testReportGeneration() throws Exception {
    ApplicationManager app = deployApplication(ReportGenerationApp.class);
    SparkManager sparkManager = app.getSparkManager(ReportGenerationSpark.class.getSimpleName()).start();
    URL url = sparkManager.getServiceURL(1, TimeUnit.MINUTES);
    Assert.assertNotNull(url);
    URL reportURL = url.toURI().resolve("reports").toURL();
    List<ReportGenerationRequest.Filter> filters =
      ImmutableList.of(
        new ReportGenerationRequest.ValueFilter<>(Constants.NAMESPACE, ImmutableList.of("ns1", "ns2"), null),
        new ReportGenerationRequest.RangeFilter<>(Constants.DURATION,
                                                  new ReportGenerationRequest.Range<>(null, 400L)));
    ReportGenerationRequest request =
      new ReportGenerationRequest(1520808000L, 1520808301L,
                                  ImmutableList.of(Constants.NAMESPACE, Constants.DURATION, Constants.USER),
                                  ImmutableList.of(
                                    new ReportGenerationRequest.Sort(Constants.DURATION,
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
