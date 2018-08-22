/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.bootstrap.executor;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.bootstrap.BootstrapStepResult;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link DefaultNamespaceCreator}.
 */
public class DefaultNamespaceCreatorTest {
  private static DefaultNamespaceCreator defaultNamespaceCreator;
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void setupClass() {
    Injector injector = AppFabricTestHelper.getInjector();
    defaultNamespaceCreator = injector.getInstance(DefaultNamespaceCreator.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
  }

  // NOTE: can't actually delete the default namespace... so everything needs to be tested in a single test case
  @Test
  public void test() throws Exception {
    try {
      namespaceAdmin.get(NamespaceId.DEFAULT);
      Assert.fail("Default namespace should not exist.");
    } catch (NamespaceNotFoundException e) {
      // expected
    }

    // test that it creates the default namespace
    BootstrapStepResult result = defaultNamespaceCreator.execute("label", new JsonObject());
    BootstrapStepResult expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);
    Assert.assertEquals(NamespaceMeta.DEFAULT, namespaceAdmin.get(NamespaceId.DEFAULT));

    // test trying to create when it's already there won't error
    result = defaultNamespaceCreator.execute("label", new JsonObject());
    expected = new BootstrapStepResult("label", BootstrapStepResult.Status.SUCCEEDED);
    Assert.assertEquals(expected, result);
    Assert.assertEquals(NamespaceMeta.DEFAULT, namespaceAdmin.get(NamespaceId.DEFAULT));
  }
}
