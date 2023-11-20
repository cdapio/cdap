/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandlerInternal;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link RemoteApplicationDetailFetcher} {@link LocalApplicationDetailFetcher} and
 * {@link AppLifecycleHttpHandlerInternal}
 */
@RunWith(Parameterized.class)
public class ApplicationDetailFetcherTest extends AppFabricTestBase {
  private enum ApplicationDetailFetcherType {
    LOCAL,
    REMOTE,
  }

  private final ApplicationDetailFetcherType fetcherType;

  public ApplicationDetailFetcherTest(ApplicationDetailFetcherType type) {
    this.fetcherType = type;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {ApplicationDetailFetcherType.LOCAL},
      {ApplicationDetailFetcherType.REMOTE},
    });
  }

  private ApplicationDetailFetcher getApplicationDetailFetcher(ApplicationDetailFetcherType type) {
    ApplicationDetailFetcher fetcher = null;
    switch (type) {
      case LOCAL:
        fetcher = AppFabricTestBase.getInjector().getInstance(LocalApplicationDetailFetcher.class);
        break;
      case REMOTE:
        fetcher = AppFabricTestBase.getInjector().getInstance(RemoteApplicationDetailFetcher.class);
        break;
    }
    return fetcher;
  }

  @Test(expected = NotFoundException.class)
  public void testGetApplicationNotFound() throws Exception {
    ApplicationDetailFetcher fetcher = getApplicationDetailFetcher(fetcherType);
    fetcher.get(new ApplicationReference(TEST_NAMESPACE1, AllProgramsApp.NAME));
  }

  /**
   * Assert the state of {@link ApplicationDetail} matches that of {@link AllProgramsApp}
   */
  private void assertAllProgramAppDetail(ApplicationDetail appDetail) {
    Assert.assertEquals(AllProgramsApp.NAME, appDetail.getName());
    Assert.assertEquals(AllProgramsApp.DESC, appDetail.getDescription());
    Assert.assertFalse(appDetail.getAppVersion().isEmpty());

    // Verify dataset names
    Assert.assertTrue(appDetail.getDatasets().size() > 0);
    List<String> datasetNames = new ArrayList<>();
    appDetail.getDatasets().forEach(datasetDetail -> datasetNames.add(datasetDetail.getName()));
    Assert.assertTrue(datasetNames.containsAll(ImmutableList.of(AllProgramsApp.DATASET_NAME,
                                                                AllProgramsApp.DATASET_NAME2,
                                                                AllProgramsApp.DATASET_NAME3)));
    // Verify program field
    Assert.assertTrue(appDetail.getPrograms().size() > 0);

    // Verify plugin field
    Assert.assertNotNull(appDetail.getPlugins());

    // Verify artifact field
    Assert.assertNotNull(appDetail.getArtifact());
  }

  @Test
  public void testGetApplication() throws Exception {
    ApplicationDetailFetcher fetcher = getApplicationDetailFetcher(fetcherType);
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);
    // Get and validate the application
    ApplicationDetail appDetail = fetcher.get(new ApplicationReference(namespace, appName));
    assertAllProgramAppDetail(appDetail);

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedApiPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testGetAllApplicationNamespaceNotFound() throws Exception {
    ApplicationDetailFetcher fetcher = getApplicationDetailFetcher(fetcherType);
    String namespace = "somenamespace";
    fetcher.scan(namespace, d -> { }, 1);
  }

  @Test
  public void testGetAllApplicationsUsingScan() throws Exception {
    ApplicationDetailFetcher fetcher = getApplicationDetailFetcher(fetcherType);
    String namespace = TEST_NAMESPACE1;
    ApplicationDetail appDetail = null;
    Integer batchSize = 1;

    // No applications have been deployed
    List<ApplicationDetail> appDetailList0 = new ArrayList<>();
    fetcher.scan(namespace, d -> appDetailList0.add(d), batchSize);
    Assert.assertEquals(Collections.emptyList(), appDetailList0);

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    List<ApplicationDetail> appDetailList1 = new ArrayList<>();
    fetcher.scan(namespace, d -> appDetailList1.add(d), batchSize);

    Assert.assertEquals(1, appDetailList1.size());
    appDetail = appDetailList1.get(0);
    assertAllProgramAppDetail(appDetail);

    // Deploy another application
    deploy(AppWithSchedule.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    List<ApplicationDetail> appDetailList2 = new ArrayList<>();
    fetcher.scan(namespace, d -> appDetailList2.add(d), batchSize);
    Assert.assertEquals(2, appDetailList2.size());
    Assert.assertEquals(AllProgramsApp.NAME, appDetailList2.get(0).getName());
    Assert.assertEquals(AllProgramsApp.DESC, appDetailList2.get(0).getDescription());
    Assert.assertEquals(AppWithSchedule.NAME, appDetailList2.get(1).getName());
    Assert.assertEquals(AppWithSchedule.DESC, appDetailList2.get(1).getDescription());

    // Delete the application
    Assert.assertEquals(
        200,
        doDelete(getVersionedApiPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}
