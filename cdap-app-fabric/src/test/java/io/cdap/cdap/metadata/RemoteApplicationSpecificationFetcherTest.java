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
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.handlers.AppLifecycleHttpHandlerInternal;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link RemoteApplicationSpecificationFetcher} and {@link AppLifecycleHttpHandlerInternal}
 */
public class RemoteApplicationSpecificationFetcherTest extends AppFabricTestBase {
  private static ApplicationSpecificationFetcher fetcher = null;

  @BeforeClass
  public static void init() {
    fetcher = getInjector().getInstance(RemoteApplicationSpecificationFetcher.class);
  }

  @Test(expected = NotFoundException.class)
  public void testGetApplicationNotFound() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;
    ApplicationId appId = new ApplicationId(namespace, appName);
    fetcher.get(appId);
  }

  /**
   * Assert the state of {@link ApplicationDetail} matches that of {@link AllProgramsApp}
   */
  private void assertAllProgramAppSpec(ApplicationSpecification appSpec) {
    Assert.assertEquals(AllProgramsApp.NAME, appSpec.getName());
    Assert.assertEquals(AllProgramsApp.DESC, appSpec.getDescription());
    Assert.assertFalse(appSpec.getAppVersion().isEmpty());

    // Verify dataset names
    Assert.assertTrue(appSpec.getDatasets().size() > 0);
    List<String> datasetNames = new ArrayList<>();
    appSpec.getDatasets().forEach((name, creationSpec) -> datasetNames.add(name));
    Assert.assertTrue(datasetNames.containsAll(ImmutableList.of(AllProgramsApp.DATASET_NAME,
                                                                AllProgramsApp.DATASET_NAME2,
                                                                AllProgramsApp.DATASET_NAME3)));
    // Verify all programs and plugins
    Assert.assertTrue(appSpec.getMapReduce().size() > 0);
    Assert.assertTrue(appSpec.getSpark().size() > 0);
    Assert.assertTrue(appSpec.getServices().size() > 0);
    Assert.assertTrue(appSpec.getWorkflows().size() > 0);
    Assert.assertTrue(appSpec.getProgramSchedules().size() > 0);
    Assert.assertTrue(appSpec.getWorkers().size() > 0);
  }

  @Test
  public void testGetApplication() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    ApplicationId appId = new ApplicationId(namespace, appName);
    ApplicationSpecification appSpec = fetcher.get(appId);
    assertAllProgramAppSpec(appSpec);

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testGetAllApplicationNamespaceNotFound() throws Exception {
    String namespace = "somenamespace";
    fetcher.list(namespace);
  }

  @Test
  public void testGetAllApplications() throws Exception {
    String namespace = TEST_NAMESPACE1;
    List<ApplicationSpecification> appSpecList = Collections.emptyList();
    ApplicationSpecification appSpec = null;

    // No applications have been deployed
    appSpecList = fetcher.list(namespace);
    Assert.assertEquals(Collections.emptyList(), appSpecList);

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    appSpecList = fetcher.list(namespace);
    Assert.assertEquals(1, appSpecList.size());
    appSpec = appSpecList.get(0);
    assertAllProgramAppSpec(appSpec);

    // Deploy another application
    deploy(AppWithSchedule.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    appSpecList = fetcher.list(namespace);
    Assert.assertEquals(2, appSpecList.size());
    Assert.assertEquals(AllProgramsApp.NAME, appSpecList.get(0).getName());
    Assert.assertEquals(AllProgramsApp.DESC, appSpecList.get(0).getDescription());
    Assert.assertEquals(AppWithSchedule.NAME, appSpecList.get(1).getName());
    Assert.assertEquals(AppWithSchedule.DESC, appSpecList.get(1).getDescription());

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}
