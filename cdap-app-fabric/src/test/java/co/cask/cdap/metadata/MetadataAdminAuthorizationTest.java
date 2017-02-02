/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.test.TestRunner;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

/**
 * Test authorization for metadata. Note this class only tests the authorization enforcement for metadata.
 * For a more comprehensive unit test for metadata which tests the logic please see
 * {@link co.cask.cdap.data2.metadata.dataset.MetadataDatasetTest} and
 * {@link co.cask.cdap.data2.metadata.store.MetadataStoreTest}
 */
@RunWith(TestRunner.class)
public class MetadataAdminAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);

  private static CConfiguration cConf;
  private static MetadataAdmin metadataAdmin;
  private static Authorizer authorizer;
  private static AppFabricServer appFabricServer;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = createCConf();
    final Injector injector = AppFabricTestHelper.getInjector(cConf);
    metadataAdmin = injector.getInstance(MetadataAdmin.class);
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
  }

  @Test
  public void testAuthorization() throws Exception {
    // test metadata search
    SecurityRequestContext.setUserId(ALICE.getName());
    authorizer.grant(new InstanceId(cConf.get(Constants.INSTANCE_NAME)), ALICE, Collections.singleton(Action.ADMIN));
    authorizer.grant(NamespaceId.DEFAULT, ALICE, Collections.singleton(Action.WRITE));

    AppFabricTestHelper.deployApplication(NamespaceId.DEFAULT.toId(), AllProgramsApp.class, "{}", cConf);
    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    EnumSet<MetadataSearchTargetType> types = EnumSet.allOf(MetadataSearchTargetType.class);
    Assert.assertFalse(
      metadataAdmin.search(NamespaceId.DEFAULT.getNamespace(), "*", types,
                           SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false).getResults().isEmpty());
    SecurityRequestContext.setUserId("bob");
    Assert.assertTrue(
      metadataAdmin.search(NamespaceId.DEFAULT.getNamespace(), "*", types,
                           SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false).getResults().isEmpty());

    // Test tag authorization
    // Bob should not be able to add a tag
    try {
      metadataAdmin.addTags(appId, "someTag");
      Assert.fail("Adding tag should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to get the tag
    try {
      metadataAdmin.getTags(MetadataScope.USER, appId);
      Assert.fail("Getting tag should have failed since bob does not have privilege on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to remove the tag
    try {
      metadataAdmin.removeTags(appId, "someTag");
      Assert.fail("Removing tag should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Test property authorization
    // Bob should not be able to add a property
    try {
      metadataAdmin.addProperties(appId, ImmutableMap.of("key1", "val1"));
      Assert.fail("Adding property should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to get a property
    try {
      metadataAdmin.getProperties(MetadataScope.USER, appId);
      Assert.fail("Getting property should have failed since bob does not have privilege on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to remove a property
    try {
      metadataAdmin.removeProperties(appId);
      Assert.fail("Removing property should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Test metadata authorization
    // Bob should not be able to get metadata
    try {
      metadataAdmin.getMetadata(MetadataScope.USER, appId);
      Assert.fail("Getting metadata should have failed since bob does not have privilege on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to remove metadata
    try {
      metadataAdmin.removeMetadata(appId);
      Assert.fail("Removing metadata should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Grant permission to Bob on entity
    SecurityRequestContext.setUserId(ALICE.getName());
    authorizer.grant(appId, BOB, Collections.singleton(Action.ADMIN));

    // Bob should now be abe to add/get/delete tag
    metadataAdmin.addTags(appId, "someTag");
    metadataAdmin.getTags(MetadataScope.USER, appId);
    metadataAdmin.removeTags(appId, "someTag");

    // Bob should be able to add/get/delete properties
    metadataAdmin.addProperties(appId, ImmutableMap.of("key1", "val1"));
    metadataAdmin.getProperties(MetadataScope.USER, appId);
    metadataAdmin.removeProperties(appId);

    // Bob should be able to get/remove metadata
    metadataAdmin.getMetadata(MetadataScope.USER, appId);
    metadataAdmin.removeMetadata(appId);
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
  }

  private static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.CACHE_ENABLED, false);
    LocationFactory locationFactory = new LocalLocationFactory(new File(TEMPORARY_FOLDER.newFolder().toURI()));
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }
}
