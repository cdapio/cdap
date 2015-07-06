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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link DefaultNamespaceAdmin}
 */
public class DefaultNamespaceAdminTest extends AppFabricTestBase {
  private static final NamespaceAdmin namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);

  @Test
  public void testNamespaces() throws AlreadyExistsException, IOException, NamespaceCannotBeCreatedException,
    NamespaceCannotBeDeletedException {
    String namespace = "namespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();

    int initialCount = namespaceAdmin.listNamespaces().size();

    // TEST_NAMESPACE_META1 is already created in AppFabricTestBase#beforeClass
    Assert.assertTrue(namespaceAdmin.hasNamespace(Id.Namespace.from(TEST_NAMESPACE1)));
    try {
      namespaceAdmin.createNamespace(TEST_NAMESPACE_META1);
      Assert.fail("Should not create duplicate namespace.");
    } catch (NamespaceAlreadyExistsException e) {
      Assert.assertEquals(Id.Namespace.from(TEST_NAMESPACE_META1.getName()), e.getId());
    }

    // "random" namespace should not exist
    try {
      namespaceAdmin.getNamespace(Id.Namespace.from("random"));
      Assert.fail("Namespace 'random' should not exist.");
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from("random"), e.getObject());
    }

    try {
      namespaceAdmin.createNamespace(null);
      Assert.fail("Namespace with null metadata should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace metadata should not be null.", e.getMessage());
    }

    Assert.assertEquals(initialCount, namespaceAdmin.listNamespaces().size());
    Assert.assertFalse(namespaceAdmin.hasNamespace(Id.Namespace.from(namespace)));

    try {
      namespaceAdmin.createNamespace(builder.build());
      Assert.fail("Namespace with no name should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace id cannot be null.", e.getMessage());
    }

    Assert.assertEquals(initialCount, namespaceAdmin.listNamespaces().size());
    Assert.assertFalse(namespaceAdmin.hasNamespace(namespaceId));

    // namespace with default fields
    namespaceAdmin.createNamespace(builder.setName(namespace).build());
    Assert.assertEquals(initialCount + 1, namespaceAdmin.listNamespaces().size());
    Assert.assertTrue(namespaceAdmin.hasNamespace(namespaceId));
    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.getNamespace(namespaceId);
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("", namespaceMeta.getDescription());

      namespaceAdmin.deleteNamespace(namespaceId);
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    namespaceAdmin.createNamespace(builder.setDescription("describes " + namespace).build());
    Assert.assertEquals(initialCount + 1, namespaceAdmin.listNamespaces().size());
    Assert.assertTrue(namespaceAdmin.hasNamespace(namespaceId));

    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.getNamespace(namespaceId);
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("describes " + namespaceId.getId(), namespaceMeta.getDescription());

      namespaceAdmin.deleteNamespace(namespaceId);
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    // Verify NotFoundException's contents as well, instead of just checking namespaceService.hasNamespace = false
    verifyNotFound(namespaceId);
  }

  private static void verifyNotFound(Id.Namespace namespaceId) {
    try {
      namespaceAdmin.getNamespace(namespaceId);
      Assert.fail(String.format("Namespace '%s' should not be found since it was just deleted", namespaceId.getId()));
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from(namespaceId.getId()), e.getId());
    }
  }
}
