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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.common.http.HttpMethod;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Tests SQL queries using {@link Connection}.
 */
public class TestSQL extends IntegrationTestBase {

  @ClassRule
  public static final StandaloneTester STANDALONE = new StandaloneTester();

  @Override
  protected String getInstanceURI() {
    return STANDALONE.getBaseURI().toString();
  }

  @Test
  public void testSQLQuery() throws Exception {
    Id.Namespace testSpace = Id.Namespace.DEFAULT;

    getTestManager().deployDatasetModule(testSpace, "my-kv", AppUsingCustomModule.Module.class);

    DatasetAdmin dsAdmin = getTestManager().addDatasetInstance(testSpace, "myKeyValueTable", "myTable");
    Assert.assertTrue(dsAdmin.exists());

    ApplicationManager appManager = deployApplication(testSpace, AppUsingCustomModule.class);
    ServiceManager serviceManager = appManager.getServiceManager("MyService").start();
    serviceManager.waitForStatus(true);

    put(serviceManager, "a", "1");
    put(serviceManager, "b", "2");
    put(serviceManager, "c", "1");

    try (
      Connection connection = getTestManager().getQueryClient(testSpace);
      // the value (character) "1" corresponds to the decimal 49. In hex, that is 31.
      ResultSet results = connection.prepareStatement("select key from dataset_mytable where hex(value) = '31'")
        .executeQuery()
    ) {
      // run a query over the dataset
      Assert.assertTrue(results.next());
      Assert.assertEquals("a", results.getString(1));
      Assert.assertTrue(results.next());
      Assert.assertEquals("c", results.getString(1));
      Assert.assertFalse(results.next());
    }

    dsAdmin.drop();
    Assert.assertFalse(dsAdmin.exists());
  }

  private void put(ServiceManager serviceManager, String key, String value) throws IOException, UnauthorizedException {
    URL url = new URL(serviceManager.getServiceURL(), key);
    getRestClient().execute(HttpMethod.PUT, url, value,
                            ImmutableMap.<String, String>of(), getClientConfig().getAccessToken());
  }
}
