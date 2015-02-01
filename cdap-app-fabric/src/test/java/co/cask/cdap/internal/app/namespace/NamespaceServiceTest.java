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

import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.NotFoundException;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link NamespaceService}
 */
public class NamespaceServiceTest extends AppFabricTestBase {
  private static final NamespaceService namespaceService = getInjector().getInstance(NamespaceService.class);

  @Test
  public void testNamespaces() {
    String namespace = "namespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();

    int initialCount = namespaceService.listNamespaces().size();

    // TEST_NAMESPACE_META1 is already created in AppFabricTestBase#beforeClass
    try {
      namespaceService.createNamespace(TEST_NAMESPACE_META1);
      Assert.fail("Should not create duplicate namespace.");
    } catch (AlreadyExistsException e) {
      Assert.assertEquals("Namespace", e.getElementType());
      Assert.assertEquals(TEST_NAMESPACE_META1.getId(), e.getElementId());
    }

    // "random" namespace should not exist
    try {
      namespaceService.getNamespace(Id.Namespace.from("random"));
      Assert.fail("Namespace 'random' should not exist.");
    } catch (NotFoundException e) {
      Assert.assertEquals("Namespace", e.getElementType());
      Assert.assertEquals("random", e.getElementId());
    }

    try {
      try {
        namespaceService.createNamespace(null);
        Assert.fail("Namespace with null metadata should fail.");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals("Namespace metadata cannot be null.", e.getMessage());
      }

      Assert.assertEquals(initialCount, namespaceService.listNamespaces().size());
      verifyNotFound(namespaceId);

      try {
        namespaceService.createNamespace(builder.build());
        Assert.fail("Namespace with no id should fail");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals("Namespace id cannot be null.", e.getMessage());
      }

      Assert.assertEquals(initialCount, namespaceService.listNamespaces().size());
      verifyNotFound(namespaceId);

      try {
        namespaceService.createNamespace(builder.setId(namespace).build());
        Assert.fail("Namespace with no name should fail");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals("Namespace name cannot be null.", e.getMessage());
      }

      Assert.assertEquals(initialCount, namespaceService.listNamespaces().size());
      verifyNotFound(namespaceId);

      try {
        namespaceService.createNamespace(builder.setName(namespace).build());
        Assert.fail("Namespace with no description should fail");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals("Namespace description cannot be null.", e.getMessage());
      }

      try {
        namespaceService.createNamespace(builder.setDescription("describes " + namespace).build());
      } catch (IllegalArgumentException e) {
        Assert.fail("Should successfully create a namespace.");
      }
    } catch (AlreadyExistsException e) {
      Assert.fail(String.format("Namespace '%s' should not exist already.", namespace));
    }

    Assert.assertEquals(initialCount + 1, namespaceService.listNamespaces().size());

    try {
      NamespaceMeta namespaceMeta = namespaceService.getNamespace(namespaceId);
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getId());
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("describes " + namespaceId.getId(), namespaceMeta.getDescription());

      namespaceService.deleteNamespace(namespaceId);
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    verifyNotFound(namespaceId);
  }

  private static void verifyNotFound(Id.Namespace namespaceId) {
    try {
      namespaceService.getNamespace(namespaceId);
      Assert.fail(String.format("Namespace '%s' should not be found since it was just deleted", namespaceId.getId()));
    } catch (NotFoundException e) {
      Assert.assertEquals("Namespace", e.getElementType());
      Assert.assertEquals(namespaceId.getId(), e.getElementId());
    }
  }
}
