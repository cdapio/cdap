/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.reactor.client.app.FakeApp;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import java.util.List;

/**
 * Test for {@link QueryClient}.
 */
@Category(XSlowTests.class)
public class QueryClientTestRun extends ClientTestBase {

  private ApplicationClient appClient;
  private QueryClient queryClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    queryClient = new QueryClient(clientConfig);
  }

  // TODO: explore query in standalone?
//  @Test
  public void testAll() throws Exception {
    appClient.deploy(createAppJarFile(FakeApp.class));

    QueryHandle queryHandle = queryClient.execute("select * from continuuity_user_" + FakeApp.DS_NAME);
    QueryStatus status = new QueryStatus(null, false);

    long startTime = System.currentTimeMillis();
    // TODO: refactor
    while (QueryStatus.OpStatus.RUNNING == status.getStatus() ||
      QueryStatus.OpStatus.INITIALIZED == status.getStatus() ||
      QueryStatus.OpStatus.PENDING == status.getStatus()) {

      Thread.sleep(1000);
      status = queryClient.getStatus(queryHandle);
    }

    Assert.assertTrue(status.hasResults());

    List<ColumnDesc> schema = queryClient.getSchema(queryHandle);
    String[] header = new String[schema.size()];
    for (int i = 0; i < header.length; i++) {
      ColumnDesc column = schema.get(i);
      // Hive columns start at 1
      int index = column.getPosition() - 1;
      header[index] = column.getName() + ": " + column.getType();
    }
    List<QueryResult> results = queryClient.getResults(queryHandle, 20);

    queryClient.delete(queryHandle);
  }
}
