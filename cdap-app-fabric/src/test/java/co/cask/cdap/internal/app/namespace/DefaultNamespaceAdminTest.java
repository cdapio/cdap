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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DefaultNamespaceAdmin}
 */
public class DefaultNamespaceAdminTest extends AppFabricTestBase {
  private static final NamespaceAdmin namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
  private static final NamespacedLocationFactory namespacedLocationFactory =
    getInjector().getInstance(NamespacedLocationFactory.class);

  @Test
  public void testNamespaces() throws Exception {
    String namespace = "namespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();

    int initialCount = namespaceAdmin.list().size();

    // TEST_NAMESPACE_META1 is already created in AppFabricTestBase#beforeClass
    Assert.assertTrue(namespaceAdmin.exists(Id.Namespace.from(TEST_NAMESPACE1)));
    try {
      namespaceAdmin.create(TEST_NAMESPACE_META1);
      Assert.fail("Should not create duplicate namespace.");
    } catch (NamespaceAlreadyExistsException e) {
      Assert.assertEquals(Id.Namespace.from(TEST_NAMESPACE_META1.getName()), e.getId());
    }

    // "random" namespace should not exist
    try {
      namespaceAdmin.get(Id.Namespace.from("random"));
      Assert.fail("Namespace 'random' should not exist.");
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from("random"), e.getObject());
    }

    try {
      namespaceAdmin.create(null);
      Assert.fail("Namespace with null metadata should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace metadata should not be null.", e.getMessage());
    }

    Assert.assertEquals(initialCount, namespaceAdmin.list().size());
    Assert.assertFalse(namespaceAdmin.exists(Id.Namespace.from(namespace)));

    try {
      namespaceAdmin.create(builder.build());
      Assert.fail("Namespace with no name should fail");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace id cannot be null.", e.getMessage());
    }

    Assert.assertEquals(initialCount, namespaceAdmin.list().size());
    Assert.assertFalse(namespaceAdmin.exists(namespaceId));

    // namespace with default fields
    namespaceAdmin.create(builder.setName(namespace).build());
    Assert.assertEquals(initialCount + 1, namespaceAdmin.list().size());
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));
    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.get(namespaceId);
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("", namespaceMeta.getDescription());

      namespaceAdmin.delete(namespaceId);
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    namespaceAdmin.create(builder.setDescription("describes " + namespace).build());
    Assert.assertEquals(initialCount + 1, namespaceAdmin.list().size());
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));

    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.get(namespaceId);
      Assert.assertEquals(namespaceId.getId(), namespaceMeta.getName());
      Assert.assertEquals("describes " + namespaceId.getId(), namespaceMeta.getDescription());

      namespaceAdmin.delete(namespaceId);
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.", namespaceId.getId()));
    }

    // Verify NotFoundException's contents as well, instead of just checking namespaceService.exists = false
    verifyNotFound(namespaceId);
  }

  @Test
  public void testConfigUpdateFailures() throws Exception {
    String namespace = "custompaceNamespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    // check that root directory for a namespace cannot be updated
    // create the custom directory since the namespace is being created with custom root directory it needs to exist
    Location customlocation = namespacedLocationFactory.get(namespaceId);
    Assert.assertTrue(customlocation.mkdirs());
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId)
      .setRootDirectory(customlocation.toString()).build();
    namespaceAdmin.create(nsMeta);
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));

    // Updating the root directory for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setRootDirectory("/newloc").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Updating the HBase namespace for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setHBaseNamespace("custns").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // Updating the hive database for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setHiveDatabase("newDB").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // removing the root directory mapping for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setRootDirectory("").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // updating the principal for an existing namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setPrincipal("newPrincipal").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // updating the keytabURI for an existing namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId().toId(),
                                      new NamespaceMeta.Builder(nsMeta).setKeytabURI("/new/keytab/uri").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    //clean up
    namespaceAdmin.delete(namespaceId);
    Locations.deleteQuietly(customlocation);
  }

  @Test
  public void testSameCustomMapping() throws Exception {

    String namespace = "custompaceNamespace";
    Id.Namespace namespaceId = Id.Namespace.from(namespace);
    // check that root directory for a namespace cannot be updated
    // create the custom directory since the namespace is being created with custom root directory it needs to exist
    Location customlocation = namespacedLocationFactory.get(namespaceId);
    Assert.assertTrue(customlocation.mkdirs());
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId)
      .setRootDirectory(customlocation.toString()).setHBaseNamespace("hbasens").setHiveDatabase("hivedb").build();
    namespaceAdmin.create(nsMeta);


    // creating a new namespace with same location should fail
    verifyAlreadyExist(new NamespaceMeta.Builder(nsMeta).setName("otherNamespace").build(), namespaceId);

    // creating a new namespace with subdir should fail.
    // subdirLocation here looks like: ..../junit/custompaceNamespace/subdir
    Location subdirLocation = customlocation.append("subdir");
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace")
                         .setRootDirectory(subdirLocation.toString()).build(), namespaceId);

    // trying to create a namespace one level up should fail
    // parentCustomLocation here looks like: ..../junit
    Location parentCustomLocation = Locations.getParent(customlocation);
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace")
                         .setRootDirectory(parentCustomLocation.toString()).build(), namespaceId);

    // but we should be able to create namespace in a different directory under same path
    // otherNamespace here looks like: ..../junit/otherNamespace
    Location otherNamespace = parentCustomLocation.append("otherNamespace");
    Assert.assertTrue(otherNamespace.mkdirs());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName("otherNamespace")
                            .setRootDirectory(otherNamespace.toString()).build());
    namespaceAdmin.delete(Id.Namespace.from("otherNamespace"));

    // creating a new namespace with same hive database should fails
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace").setHiveDatabase("hivedb").build(),
                       namespaceId);

    // creating a new namespace with same hbase namespace should fail
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace").setHBaseNamespace("hbasens").build(),
                       namespaceId);
  }

  private static void verifyAlreadyExist(NamespaceMeta namespaceMeta, Id.Namespace existingNamespace)
    throws Exception {
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail(String.format("Namespace '%s' should not have been created", namespaceMeta.getName()));
    } catch (NamespaceAlreadyExistsException e) {
      Assert.assertEquals(existingNamespace, e.getId());
    }
  }

  private static void verifyNotFound(Id.Namespace namespaceId) throws Exception {
    try {
      namespaceAdmin.get(namespaceId);
      Assert.fail(String.format("Namespace '%s' should not be found since it was just deleted", namespaceId.getId()));
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(Id.Namespace.from(namespaceId.getId()), e.getId());
    }
  }
}
