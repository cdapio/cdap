/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for {@link QueryClient}.
 */
@Category(XSlowTests.class)
@Ignore
public class QueryClientTestRun extends ClientTestBase {

  private ApplicationClient appClient;
  private QueryClient queryClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    queryClient = new QueryClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    appClient.deploy(createAppJarFile(FakeApp.class));

    QueryHandle queryHandle = queryClient.execute("select * from cdap_user_" + FakeApp.DS_NAME);
    QueryStatus status = new QueryStatus(null, false);

    while (QueryStatus.OpStatus.RUNNING == status.getStatus() ||
      QueryStatus.OpStatus.INITIALIZED == status.getStatus() ||
      QueryStatus.OpStatus.PENDING == status.getStatus()) {

      Thread.sleep(1000);
      status = queryClient.getStatus(queryHandle);
    }

    Assert.assertFalse(status.hasResults());
    Assert.assertNotNull(queryClient.getSchema(queryHandle));
    Assert.assertNotNull(queryClient.getResults(queryHandle, 20));
    queryClient.delete(queryHandle);
  }
}
