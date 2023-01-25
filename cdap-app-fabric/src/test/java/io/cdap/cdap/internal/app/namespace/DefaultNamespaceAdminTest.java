/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.namespace;

import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceAlreadyExistsException;
import io.cdap.cdap.common.NamespaceCannotBeDeletedException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.RepositoryNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.tethering.NamespaceAllocation;
import io.cdap.cdap.internal.tethering.PeerInfo;
import io.cdap.cdap.internal.tethering.PeerMetadata;
import io.cdap.cdap.internal.tethering.TetheringStatus;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Tests for {@link DefaultNamespaceAdmin}
 */
public class DefaultNamespaceAdminTest extends AppFabricTestBase {
  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static LocationFactory baseLocationFactory;
  private static NamespacePathLocator namespacePathLocator;
  private static TetheringStore tetheringStore;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = createBasicCConf();
    // we enable Kerberos for these unit tests, so we can test namespace group permissions (see testDataDirCreation).
    cConf.set(Constants.Security.KERBEROS_ENABLED, Boolean.toString(true));
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "cdap");
    initializeAndStartServices(cConf);

    namespaceAdmin = getInjector().getInstance(NamespaceAdmin.class);
    baseLocationFactory = getInjector().getInstance(LocationFactory.class);
    namespacePathLocator =
      getInjector().getInstance(NamespacePathLocator.class);
    tetheringStore = getInjector().getInstance(TetheringStore.class);
  }

  @Test
  public void testNamespaces() throws Exception {
    String namespace = "namespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();

    int initialCount = namespaceAdmin.list().size();

    // TEST_NAMESPACE_META1 is already created in AppFabricTestBase#beforeClass
    Assert.assertTrue(namespaceAdmin.exists(new NamespaceId(TEST_NAMESPACE1)));
    // It should be present in cache too
    Assert.assertNotNull(getFromCache(new NamespaceId(TEST_NAMESPACE1)));
    try {
      namespaceAdmin.create(TEST_NAMESPACE_META1);
      Assert.fail("Should not create duplicate namespace.");
    } catch (NamespaceAlreadyExistsException e) {
      Assert.assertEquals(TEST_NAMESPACE_META1.getNamespaceId(), e.getId());
    }

    // "random" namespace should not exist
    try {
      namespaceAdmin.get(new NamespaceId("random"));
      Assert.fail("Namespace 'random' should not exist.");
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(new NamespaceId("random"), e.getId());
    }

    try {
      namespaceAdmin.create(null);
      Assert.fail("Namespace with null metadata should fail.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Namespace metadata should not be null.", e.getMessage());
    }

    Assert.assertEquals(initialCount, namespaceAdmin.list().size());
    Assert.assertFalse(namespaceAdmin.exists(new NamespaceId(namespace)));

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
    // it should be loaded in cache too since exists calls get
    Assert.assertNotNull(getFromCache(namespaceId));
    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.get(namespaceId);
      Assert.assertEquals(namespaceId.getNamespace(), namespaceMeta.getName());
      Assert.assertEquals("", namespaceMeta.getDescription());

      namespaceAdmin.delete(namespaceId);
      // it should be deleted from the cache too
      Assert.assertNull(getFromCache(namespaceId));
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.",
                                namespaceId.getNamespace()));
    }

    namespaceAdmin.create(builder.setDescription("describes " + namespace).build());
    Assert.assertEquals(initialCount + 1, namespaceAdmin.list().size());
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));

    try {
      NamespaceMeta namespaceMeta = namespaceAdmin.get(namespaceId);
      // it should be loaded in cache too
      Assert.assertNotNull(getFromCache(namespaceId));
      Assert.assertEquals(namespaceId.getNamespace(), namespaceMeta.getName());
      Assert.assertEquals("describes " + namespaceId.getNamespace(), namespaceMeta.getDescription());

      namespaceAdmin.delete(namespaceId);
      // it should be deleted from the cache
      Assert.assertNull(getFromCache(namespaceId));
    } catch (NotFoundException e) {
      Assert.fail(String.format("Namespace '%s' should be found since it was just created.",
                                namespaceId.getNamespace()));
    }

    // Verify NotFoundException's contents as well, instead of just checking namespaceService.exists = false
    verifyNotFound(namespaceId);
  }

  @Test
  public void testNamespaceUsedInTethering() throws Exception {
    // Create 2 namespaces
    String namespace1 = "namespace1";
    NamespaceId namespaceId1 = new NamespaceId(namespace1);
    String namespace2 = "namespace2";
    NamespaceId namespaceId2 = new NamespaceId(namespace2);
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace1).build());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespace2).build());

    // namespace2 is used in a tethering connection, but namespace1 is not
    PeerMetadata metadata = new PeerMetadata(Collections.singletonList(new NamespaceAllocation(namespace2,
                                                                                               null,
                                                                                               null)),
                                                                       Collections.emptyMap(), null);
    PeerInfo peerInfo = new PeerInfo("peer", null, TetheringStatus.ACCEPTED, metadata, 0);
    tetheringStore.addPeer(peerInfo);

    // Deletion of namespace1 should be successful
    namespaceAdmin.delete(namespaceId1);

    // Deletion of namespace2 should fail as it's associated with a tethering connection
    try {
      namespaceAdmin.delete(namespaceId2);
      Assert.fail();
    } catch (NamespaceCannotBeDeletedException e) {
      // expected
    }

    // Deletion of namespace2 should succeed after the tethering is deleted
    tetheringStore.deletePeer("peer");
    namespaceAdmin.delete(namespaceId2);
  }

  @Test
  public void testKerberos() throws Exception {
    // test that the namespace create handler doesn't allow configuring only one of the following two:
    // principal, keytabURI
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName("test_ns").setPrincipal("somePrincipal").build();
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail();
    } catch (BadRequestException bre) {
      // expected
    }

    // now check with just key tab uri
    namespaceMeta = new NamespaceMeta.Builder().setName("test_ns").setKeytabURI("/some/path").build();
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail();
    } catch (BadRequestException bre) {
      // expected
    }

    namespaceMeta = new NamespaceMeta.Builder().setName("test_ns").setKeytabURI("/some/path")
      .build();
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail();
    } catch (BadRequestException bre) {
      // expected
    }

    namespaceMeta = new NamespaceMeta.Builder().setName("test_ns").setPrincipal("somePrincipal")
      .build();
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail();
    } catch (BadRequestException bre) {
      // expected
    }
  }

  @Test
  public void testConfigUpdate() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    // check that root directory for a namespace cannot be updated
    // create the custom directory since the namespace is being created with custom root directory it needs to exist
    String customRoot = "/some/custom/dir";
    Location customlocation = baseLocationFactory.create(customRoot);
    Assert.assertTrue(customlocation.mkdirs());
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId)
      .setRootDirectory(customRoot).build();
    namespaceAdmin.create(nsMeta);
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));

    // Updating the root directory for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setRootDirectory("/newloc").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Updating the HBase namespace for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setHBaseNamespace("custns").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // Updating the hive database for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setHiveDatabase("newDB").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // removing the root directory mapping for a namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setRootDirectory("").build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // updating the principal for an existing namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setPrincipal("newPrincipal").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // updating the keytabURI for an existing namespace with no existing principal should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setKeytabURI("/new/keytab/uri").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    // updating the groupname for an existing namespace should fail
    try {
      namespaceAdmin.updateProperties(nsMeta.getNamespaceId(),
                                      new NamespaceMeta.Builder(nsMeta).setGroupName("anotherGroup").build());
      Assert.fail();
    } catch (BadRequestException e) {
      // expected
    }

    //clean up
    namespaceAdmin.delete(namespaceId);
    Locations.deleteQuietly(customlocation);
  }

  @Test
  public void testSetRepoConfig() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setRepository(namespaceRepo).build();
    namespaceAdmin.create(nsMeta);
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));

    RepositoryConfig newRepositoryConfig = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("another.example.com").setDefaultBranch("master").setAuthType(AuthType.PAT)
      .setTokenName("another.token").setUsername("another.user").build();
    namespaceAdmin.setRepository(nsMeta.getNamespaceId(), newRepositoryConfig);

    NamespaceMeta updatedNamespaceMeta = namespaceAdmin.get(namespaceId);
    Assert.assertEquals(newRepositoryConfig, updatedNamespaceMeta.getRepository());

    // Setting a null RepositoryConfig returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(), null);
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig Provider returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(),
                                   new RepositoryConfig.Builder(namespaceRepo).setProvider(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig Link returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(),
                                   new RepositoryConfig.Builder(namespaceRepo).setLink(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig DefaultBranch returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(),
                                   new RepositoryConfig.Builder(namespaceRepo).setDefaultBranch(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig TokenName returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(),
                                   new RepositoryConfig.Builder(namespaceRepo).setTokenName(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    // Setting a null RepositoryConfig AuthType returns 400
    try {
      namespaceAdmin.setRepository(nsMeta.getNamespaceId(),
                                   new RepositoryConfig.Builder(namespaceRepo).setAuthType(null).build());
      Assert.fail();
    } catch (BadRequestException e) {
      //expected
    }

    //clean up
    namespaceAdmin.delete(namespaceId);
  }

  @Test
  public void testDeleteRepoConfig() throws Exception {
    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    RepositoryConfig namespaceRepo = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink("example.com").setDefaultBranch("develop").setAuthType(AuthType.PAT)
      .setTokenName("token").setUsername("user").build();
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId).setRepository(namespaceRepo).build();
    namespaceAdmin.create(nsMeta);
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));
    NamespaceMeta namespaceMeta = namespaceAdmin.get(namespaceId);
    Assert.assertEquals(namespaceRepo, namespaceMeta.getRepository());

    // Setting a null RepositoryConfig returns 400
    try {
      String nonExistsNs = "NonExistsNamespace";
      namespaceAdmin.deleteRepository(new NamespaceId(nonExistsNs));
      Assert.fail();
    } catch (NamespaceNotFoundException e) {
      //expected
    }

    namespaceAdmin.deleteRepository(nsMeta.getNamespaceId());

    // Setting a null RepositoryConfig Provider returns 400
    try {
      namespaceAdmin.deleteRepository(nsMeta.getNamespaceId());
      Assert.fail();
    } catch (RepositoryNotFoundException e) {
      //expected
    }

    //clean up
    namespaceAdmin.delete(namespaceId);
  }

  @Test
  public void testUpdateExistingKeytab() throws Exception {
    String namespace = "updateNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId)
      .setPrincipal("alice").setKeytabURI("/alice/keytab").build();
    namespaceAdmin.create(nsMeta);
    Assert.assertTrue(namespaceAdmin.exists(namespaceId));
    // update the keytab URI
    String newKeytab = "/alice/new_keytab";
    NamespaceMeta newKeytabMeta = new NamespaceMeta.Builder(nsMeta).setKeytabURI(newKeytab).build();
    namespaceAdmin.updateProperties(nsMeta.getNamespaceId(), newKeytabMeta);
    // assert the keytab URI is updated and the version remains 0
    Assert.assertEquals(newKeytab, namespaceAdmin.get(namespaceId).getConfig().getKeytabURIWithoutVersion());
    Assert.assertEquals(0, namespaceAdmin.get(namespaceId).getConfig().getKeytabURIVersion());
    // update the namespace with the same keytab URI
    namespaceAdmin.updateProperties(nsMeta.getNamespaceId(), newKeytabMeta);
    // assert the keytab URI without version remains the same and the version is incremented to 1
    Assert.assertEquals(newKeytab, namespaceAdmin.get(namespaceId).getConfig().getKeytabURIWithoutVersion());
    Assert.assertEquals(1, namespaceAdmin.get(namespaceId).getConfig().getKeytabURIVersion());
    //clean up
    namespaceAdmin.delete(namespaceId);
  }

  @Test
  public void testSameCustomMapping() throws Exception {

    String namespace = "custompaceNamespace";
    NamespaceId namespaceId = new NamespaceId(namespace);
    // create the custom directory since the namespace is being created with custom root directory it needs to exist
    String parentPath = "/custom/root";
    String customRootPath = parentPath + "/path";
    Location customlocation = baseLocationFactory.create(customRootPath);
    Assert.assertTrue(customlocation.mkdirs());
    NamespaceMeta nsMeta = new NamespaceMeta.Builder().setName(namespaceId)
      .setRootDirectory(customRootPath).setHBaseNamespace("hbasens").setHiveDatabase("hivedb").build();
    namespaceAdmin.create(nsMeta);

    // creating a new namespace with same location should fail
    verifyAlreadyExist(new NamespaceMeta.Builder(nsMeta).setName("otherNamespace").build(), namespaceId);

    // creating a new namespace with subdir should fail.
    // subdirLocation here looks like: ..../junit/custompaceNamespace/subdir
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace")
                         .setRootDirectory(customRootPath + "/subdir").build(), namespaceId);

    // trying to create a namespace one level up should fail
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace")
                         .setRootDirectory(parentPath).build(), namespaceId);

    // but we should be able to create namespace in a different directory under same path
    // otherNamespace here looks like: ..../junit/otherNamespace
    String otherRoot = parentPath + "/otherpath";
    Location otherNamespace = baseLocationFactory.create(otherRoot);
    Assert.assertTrue(otherNamespace.mkdirs());
    namespaceAdmin.create(new NamespaceMeta.Builder().setName("otherNamespace")
                            .setRootDirectory(otherRoot).build());
    namespaceAdmin.delete(new NamespaceId("otherNamespace"));

    // creating a new namespace with same hive database should fails
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace").setHiveDatabase("hivedb").build(),
                       namespaceId);

    // creating a new namespace with same hbase namespace should fail
    verifyAlreadyExist(new NamespaceMeta.Builder().setName("otherNamespace").setHBaseNamespace("hbasens").build(),
                       namespaceId);

    //clean up
    namespaceAdmin.delete(namespaceId);
    Locations.deleteQuietly(customlocation);
  }

  @Test
  public void testDataDirCreation() throws Exception {
    // create a namespace with default settings, validate that data dir exists and has
    namespaceAdmin.create(new NamespaceMeta.Builder().setName("dd1").build());

    Location homeDir = namespacePathLocator.get(new NamespaceId("dd1"));
    Location dataDir = homeDir.append(Constants.Dataset.DEFAULT_DATA_DIR);
    Location tempDir = homeDir.append(cConf.get(Constants.AppFabric.TEMP_DIR));

    for (Location loc : new Location[] { homeDir, dataDir, tempDir }) {
      Assert.assertTrue(loc.exists());
      Assert.assertEquals(UserGroupInformation.getCurrentUser().getPrimaryGroupName(), loc.getGroup());
    }

    // Determine a group other than the current user's primary group to use for testing
    // Note: this is only meaningful if the user running this test is in at least 2 groups
    // Note: In some environments, UGI has groups that the user does not actually belong to...
    //       these appear to be at the end of the group list, so picking the second groups seems safest.
    String[] groups = UserGroupInformation.getCurrentUser().getGroupNames();
    Assert.assertTrue(groups.length > 0);
    String nsGroup = groups[groups.length > 1 ? 1 : 0];

    // create and validate a namespace with a default settings except that a group is configured
    namespaceAdmin.create(new NamespaceMeta.Builder().setName("dd2").setGroupName(nsGroup).build());

    homeDir = namespacePathLocator.get(new NamespaceId("dd2"));
    dataDir = homeDir.append(Constants.Dataset.DEFAULT_DATA_DIR);
    tempDir = homeDir.append(cConf.get(Constants.AppFabric.TEMP_DIR));

    Assert.assertTrue(homeDir.exists());
    Assert.assertEquals(nsGroup, homeDir.getGroup());
    for (Location loc : new Location[] { dataDir, tempDir }) {
      Assert.assertTrue(loc.exists());
      Assert.assertEquals(nsGroup, loc.getGroup());
      Assert.assertEquals("rwx", loc.getPermissions().substring(3, 6));
    }

    // for a custom root, but no group configured, the data dir inherits the group from the root
    String basePath = "/custom/dd3";
    homeDir = baseLocationFactory.create(basePath);
    Assert.assertTrue(homeDir.mkdirs());
    String homeGroup = homeDir.getGroup();

    namespaceAdmin.create(new NamespaceMeta.Builder().setName("dd3").setRootDirectory(basePath).build());

    dataDir = homeDir.append(Constants.Dataset.DEFAULT_DATA_DIR);
    tempDir = homeDir.append(cConf.get(Constants.AppFabric.TEMP_DIR));

    for (Location loc : new Location[] { homeDir, dataDir, tempDir }) {
      Assert.assertTrue(loc.exists());
      Assert.assertEquals(homeGroup, loc.getGroup());
    }

    // for a custom root and a group configured, the data dir gets the custom group and group 'rwx'
    basePath = "/custom/dd4";
    homeDir = baseLocationFactory.create(basePath);
    Assert.assertTrue(homeDir.mkdirs());
    String homePermissions = homeDir.getPermissions();

    namespaceAdmin.create(new NamespaceMeta.Builder().setName("dd4")
                            .setGroupName(nsGroup).setRootDirectory(basePath).build());

    dataDir = homeDir.append(Constants.Dataset.DEFAULT_DATA_DIR);
    tempDir = homeDir.append(cConf.get(Constants.AppFabric.TEMP_DIR));

    // home dir should have existing group and permissions
    Assert.assertTrue(homeDir.exists());
    Assert.assertEquals(homeGroup, homeDir.getGroup());
    Assert.assertEquals(homePermissions, homeDir.getPermissions());
    for (Location loc : new Location[] { dataDir, tempDir }) {
      Assert.assertTrue(loc.exists());
      Assert.assertEquals(nsGroup, loc.getGroup());
      Assert.assertEquals("rwx", loc.getPermissions().substring(3, 6));
    }
  }

  @Nullable
  private NamespaceMeta getFromCache(NamespaceId namespaceId) {
    return ((DefaultNamespaceAdmin) namespaceAdmin).getCache().get(namespaceId);
  }

  private static void verifyAlreadyExist(NamespaceMeta namespaceMeta, NamespaceId existingNamespace)
    throws Exception {
    try {
      namespaceAdmin.create(namespaceMeta);
      Assert.fail(String.format("Namespace '%s' should not have been created", namespaceMeta.getName()));
    } catch (BadRequestException e) {
      Assert.assertTrue(e.getMessage().contains(existingNamespace.getNamespace()));
    }
  }

  private static void verifyNotFound(NamespaceId namespaceId) throws Exception {
    try {
      namespaceAdmin.get(namespaceId);
      Assert.fail(String.format("Namespace '%s' should not be found since it was just deleted",
                                namespaceId.getNamespace()));
    } catch (NamespaceNotFoundException e) {
      Assert.assertEquals(namespaceId, e.getId());
    }
  }
}
