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

package co.cask.cdap.test.basics;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ApplicationRecord;
import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.app.FakeApp;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 *
 */
public class ApplicationLifecycleTestRun extends IntegrationTestBase {

  @Test
  public void testDeployAndDelete() throws Exception {
    ApplicationClient applicationClient = getApplicationClient();
    File appJarFile = createAppJarFile(FakeApp.class);

    try {
      applicationClient.deploy(appJarFile);
      List<ApplicationRecord> applicationRecords = applicationClient.list();
      Assert.assertEquals(1, applicationRecords.size());
      Assert.assertEquals(FakeApp.NAME, applicationRecords.get(0).getId());

      applicationClient.delete(FakeApp.NAME);
      applicationRecords = applicationClient.list();
      Assert.assertEquals(0, applicationRecords.size());
    } finally {
      tryDeleteApp(FakeApp.NAME);
    }
  }

}
