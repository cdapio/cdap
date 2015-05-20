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

package co.cask.cdap.test;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.common.exception.ProgramNotFoundException;
import co.cask.cdap.common.exception.UnauthorizedException;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.StreamDetail;
import co.cask.cdap.security.authentication.client.AccessToken;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 *
 */
public class IntegrationTestBase {

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(new StandaloneTester());

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * If empty, start our own CDAP standalone instance for testing.
   * If not empty, use the provided remote CDAP instance for testing.
   */
  private static final String INSTANCE_URI = System.getProperty("instanceUri", "");

  /**
   * CDAP access token for making requests to secure CDAP instances.
   * Can be obtained via the CDAP authentication server.
   */
  private static final String ACCESS_TOKEN = System.getProperty("accessToken", "");

  @Before
  public void setUp() throws Exception {
    assertNoApps();
    assertNoUserDatasets();
    // TODO: check metrics, streams, etc.

    // TODO: check no streams once streams can be deleted instead of truncating all streams
    StreamClient streamClient = getStreamClient();
    List<StreamDetail> streamRecords = streamClient.list();
    if (streamRecords.size() > 0) {
      for (StreamDetail streamRecord : streamRecords) {
        try {
          streamClient.truncate(streamRecord.getName());
        } catch (Exception e) {
          Assert.fail("All existing streams must be truncated" +
                      " - failed to truncate stream '" + streamRecord.getName() + "'");
        }
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    getTestManager().clear();
    assertNoApps();
    assertNoUserDatasets();
    // TODO: check metrics, streams, etc.
  }

  protected static TestManager getTestManager() {
    try {
      return new IntegrationTestManager(getClientConfig(), new LocalLocationFactory(TEMP_FOLDER.newFolder()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  protected static ClientConfig getClientConfig() {
    StandaloneTester standalone = STANDALONE.get();
    ClientConfig.Builder builder = new ClientConfig.Builder();
    if (INSTANCE_URI.isEmpty()) {
      builder.setConnectionConfig(InstanceURIParser.DEFAULT.parse(
        standalone.getBaseURI().toString()));
    } else {
      builder.setConnectionConfig(InstanceURIParser.DEFAULT.parse(
        URI.create(INSTANCE_URI).toString()));
    }

    if (!ACCESS_TOKEN.isEmpty()) {
      builder.setAccessToken(new AccessToken(ACCESS_TOKEN, 0L, null));
    }

    builder.setDefaultConnectTimeout(120000);
    builder.setDefaultReadTimeout(120000);
    builder.setUploadConnectTimeout(0);
    builder.setUploadConnectTimeout(0);

    return builder.build();
  }

  protected MetaClient getMetaClient() {
    return new MetaClient(getClientConfig());
  }

  protected ApplicationClient getApplicationClient() {
    return new ApplicationClient(getClientConfig());
  }

  protected ProgramClient getProgramClient() {
    return new ProgramClient(getClientConfig());
  }

  protected StreamClient getStreamClient() {
    return new StreamClient(getClientConfig());
  }

  protected DatasetClient getDatasetClient() {
    return new DatasetClient(getClientConfig());
  }

  protected ApplicationManager deployApplication(Id.Namespace namespace,
                                                 Class<? extends Application> applicationClz,
                                                 File...bundleEmbeddedJars) throws IOException {
    return getTestManager().deployApplication(namespace, applicationClz, bundleEmbeddedJars);
  }

  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                                 File...bundleEmbeddedJars) throws IOException {
    return deployApplication(getClientConfig().getNamespace(), applicationClz, bundleEmbeddedJars);
  }

  protected ApplicationManager deployApplication(Id.Namespace namespace,
                                                 Class<? extends Application> applicationClz) throws IOException {
    return deployApplication(namespace, applicationClz, new File[0]);
  }

  protected ApplicationManager deployApplication(Class<? extends Application> applicationClz) throws IOException {
    return deployApplication(getClientConfig().getNamespace(), applicationClz, new File[0]);
  }

  private boolean isUserDataset(DatasetSpecificationSummary specification) {
    final DefaultDatasetNamespace dsNamespace = new DefaultDatasetNamespace(CConfiguration.create());
    return !dsNamespace.contains(specification.getName(), Constants.SYSTEM_NAMESPACE);
  }

  private void assertNoUserDatasets() throws Exception {
    DatasetClient datasetClient = getDatasetClient();
    List<DatasetSpecificationSummary> datasets = datasetClient.list();

    Iterable<DatasetSpecificationSummary> filteredDatasts = Iterables.filter(
      datasets, new Predicate<DatasetSpecificationSummary>() {
      @Override
      public boolean apply(@Nullable DatasetSpecificationSummary input) {
        if (input == null) {
          return true;
        }

        return isUserDataset(input);
      }
    });

    Iterable<String> filteredDatasetsNames = Iterables.transform(
      filteredDatasts, new Function<DatasetSpecificationSummary, String>() {
      @Nullable
      @Override
      public String apply(@Nullable DatasetSpecificationSummary input) {
        if (input == null) {
          throw new IllegalStateException();
        }

        return input.getName();
      }
    });

    Assert.assertFalse("Must have no user datasets, but found the following user datasets: "
                         + Joiner.on(", ").join(filteredDatasetsNames), filteredDatasts.iterator().hasNext());
  }

  private void assertNoApps() throws Exception {
    ApplicationClient applicationClient = getApplicationClient();
    List<ApplicationRecord> applicationRecords = applicationClient.list();
    List<String> applicationIds = Lists.newArrayList();
    for (ApplicationRecord applicationRecord : applicationRecords) {
      applicationIds.add(applicationRecord.getName());
    }

    Assert.assertEquals("Must have no deployed apps, but found the following apps: "
                        + Joiner.on(", ").join(applicationIds), 0, applicationRecords.size());
  }

  private void verifyProgramNames(List<String> expected, List<ProgramRecord> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramRecord actualProgramRecord : actual) {
      Assert.assertTrue(expected.contains(actualProgramRecord.getName()));
    }
  }

  private void verifyProgramNames(List<String> expected, Map<ProgramType, List<ProgramRecord>> actual) {
    verifyProgramNames(expected, convert(actual));
  }

  private List<ProgramRecord> convert(Map<ProgramType, List<ProgramRecord>> map) {
    List<ProgramRecord> result = Lists.newArrayList();
    for (List<ProgramRecord> subList : map.values()) {
      result.addAll(subList);
    }
    return result;
  }

  private void assertFlowletInstances(ProgramClient programClient, String appId, String flowId, String flowletId,
                                        int numInstances)
    throws IOException, NotFoundException, UnauthorizedException {

    // TODO: replace with programClient.waitForFlowletInstances()
    int actualInstances;
    int numTries = 0;
    int maxTries = 5;
    do {
      actualInstances = programClient.getFlowletInstances(appId, flowId, flowletId);
      numTries++;
    } while (actualInstances != numInstances && numTries <= maxTries);
    Assert.assertEquals(numInstances, actualInstances);
  }

  private void assertProgramRunning(ProgramClient programClient, String appId, ProgramType programType,
                                      String programId)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    assertProgramStatus(programClient, appId, programType, programId, "RUNNING");
  }

  private void assertProgramStopped(ProgramClient programClient, String appId, ProgramType programType,
                                      String programId)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    assertProgramStatus(programClient, appId, programType, programId, "STOPPED");
  }

  private void assertProgramStatus(ProgramClient programClient, String appId, ProgramType programType,
                                     String programId, String programStatus)
    throws IOException, ProgramNotFoundException, UnauthorizedException, InterruptedException {

    try {
      programClient.waitForStatus(appId, programType, programId, programStatus, 30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // NO-OP
    }

    Assert.assertEquals(programStatus, programClient.getStatus(appId, programType, programId));
  }
}
