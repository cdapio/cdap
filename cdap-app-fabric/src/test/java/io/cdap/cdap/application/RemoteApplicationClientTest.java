/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.application;

import com.google.inject.Injector;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class RemoteApplicationClientTest extends AppFabricTestBase {

  private static Injector injector;

  @BeforeClass
  public static void setup() {
    injector = AppFabricTestHelper.getInjector();
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testList() throws Exception {
    deploy(AppWithServices.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    AbstractApplicationClient applicationClient = injector.getInstance(RemoteApplicationClient.class);
    List<ApplicationRecord> applications = applicationClient.list(NamespaceId.DEFAULT);
    Assert.assertEquals(1, applications.size());

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithServices.NAME));
  }

  @Test
  public void testGet() throws Exception {
    deploy(AppWithServices.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    AbstractApplicationClient applicationClient = injector.getInstance(RemoteApplicationClient.class);
    ApplicationDetail applicationDetail = applicationClient.get(NamespaceId.DEFAULT.app(AppWithServices.NAME));
    Assert.assertNotNull(applicationDetail);

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithServices.NAME));
  }

  private void deleteAppAndData(ApplicationId applicationId) throws Exception {
    deleteApp(applicationId, 200);
    deleteNamespaceData(applicationId.getNamespace());
  }

}
