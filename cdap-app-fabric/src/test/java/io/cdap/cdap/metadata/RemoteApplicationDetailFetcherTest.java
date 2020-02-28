/*
ru * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test for {@link RemoteApplicationDetailFetcher}.
 */
public class RemoteApplicationDetailFetcherTest extends AppFabricTestBase {
  private static AbstractApplicationDetailFetcher fetcher = null;

  @BeforeClass
  public static void init() {
    fetcher = getInjector().getInstance(RemoteApplicationDetailFetcher.class);
  }

  @Test(expected = NotFoundException.class)
  public void testGetApplicationNotFound() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;
    ApplicationId appId = new ApplicationId(namespace, appName);
    fetcher.get(appId);
  }

  @Test
  public void testGetApplication() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    ApplicationId appId = new ApplicationId(namespace, appName);
    ApplicationDetail appDetail = fetcher.get(appId);
    Assert.assertEquals(AllProgramsApp.NAME, appDetail.getName());
    Assert.assertEquals(AllProgramsApp.DESC, appDetail.getDescription());

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test(expected = NotFoundException.class)
  public void testGetAllApplicationNamespaceNotFound() throws Exception {
    String namespace = "somenamespace";
    fetcher.list(namespace);
  }

  @Test
  public void testGetAllApplications() throws Exception {
    String namespace = TEST_NAMESPACE1;
    List<ApplicationDetail> appDetailList = Collections.emptyList();

    // No applications have been deployed
    appDetailList = fetcher.list(namespace);
    Assert.assertEquals(Collections.emptyList(), appDetailList);

    // Deploy the application
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    appDetailList = fetcher.list(namespace);
    Assert.assertEquals(1, appDetailList.size());
    Assert.assertEquals(AllProgramsApp.NAME, appDetailList.get(0).getName());
    Assert.assertEquals(AllProgramsApp.DESC, appDetailList.get(0).getDescription());

    // Deploy another application
    deploy(AppWithSchedule.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get and validate the application
    appDetailList = fetcher.list(namespace);
    Assert.assertEquals(2, appDetailList.size());
    Assert.assertEquals(AllProgramsApp.NAME, appDetailList.get(0).getName());
    Assert.assertEquals(AllProgramsApp.DESC, appDetailList.get(0).getDescription());
    Assert.assertEquals(AppWithSchedule.NAME, appDetailList.get(1).getName());
    Assert.assertEquals(AppWithSchedule.DESC, appDetailList.get(1).getDescription());

    // Delete the application
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}
