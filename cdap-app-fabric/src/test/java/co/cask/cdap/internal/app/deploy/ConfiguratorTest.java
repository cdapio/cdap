/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.WordCountApp;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests the configurators.
 *
 * NOTE: Till we can build the JAR it's difficult to test other configurators
 * {@link co.cask.cdap.internal.app.deploy.InMemoryConfigurator} &
 * {@link co.cask.cdap.internal.app.deploy.SandboxConfigurator}
 */
public class ConfiguratorTest {

  @Test
  public void testInMemoryConfigurator() throws Exception {
    Location appJar = AppFabricTestHelper.createAppJar(WordCountApp.class);

    // Create a configurator that is testable. Provide it a application.
    Configurator configurator = new InMemoryConfigurator(DefaultId.NAMESPACE, appJar);

    // Extract response from the configurator.
    ListenableFuture<ConfigResponse> result = configurator.config();
    ConfigResponse response = result.get(10, TimeUnit.SECONDS);
    Assert.assertNotNull(response);

    // Deserialize the JSON spec back into Application object.
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification specification = adapter.fromJson(response.get());
    Assert.assertNotNull(specification);
    Assert.assertTrue(specification.getName().equals("WordCountApp")); // Simple checks.
    Assert.assertTrue(specification.getFlows().size() == 1); // # of flows.
  }

}
