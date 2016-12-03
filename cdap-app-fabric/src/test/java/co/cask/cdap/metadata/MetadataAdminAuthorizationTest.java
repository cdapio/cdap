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
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.proto.Id;
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
import java.util.Set;

/**
 * Test authorization for metadata
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
  public void testSearch() throws Exception {
    SecurityRequestContext.setUserId(ALICE.getName());
    authorizer.grant(new InstanceId(cConf.get(Constants.INSTANCE_NAME)), ALICE, Collections.singleton(Action.ADMIN));
    authorizer.grant(NamespaceId.DEFAULT, ALICE, Collections.singleton(Action.WRITE));
    AppFabricTestHelper.deployApplication(NamespaceId.DEFAULT.toId(), AllProgramsApp.class, "{}", cConf);
    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    Assert.assertFalse(metadataAdmin.searchMetadata(NamespaceId.DEFAULT.getNamespace(), "*",
            EnumSet.allOf(MetadataSearchTargetType.class)).isEmpty());
    SecurityRequestContext.setUserId("bob");
    Assert.assertTrue(metadataAdmin.searchMetadata(NamespaceId.DEFAULT.getNamespace(), "*",
            EnumSet.allOf(MetadataSearchTargetType.class)).isEmpty());

    // Bob should not be able to add a tag
    try {
      metadataAdmin.addTags(appId, "someTag");
      Assert.fail("Adding tag should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Alice should be able to add a tag
    SecurityRequestContext.setUserId(ALICE.getName());
    metadataAdmin.addTags(appId, "someTag");
    Set<String> tags = metadataAdmin.getTags(MetadataScope.USER, appId);
    Assert.assertTrue(tags.contains("someTag"));

    // Bob should not be able to get the tag since he does not have READ on the entity
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      metadataAdmin.getTags(MetadataScope.USER, appId);
      Assert.fail("Adding tag should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Bob should not be able to remove the tag since he does not have ADMIN on the entity
    try {
      metadataAdmin.removeTags(appId, "someTag");
      Assert.fail("Adding tag should have failed since bob does not have ADMIN on entity");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof UnauthorizedException);
    }

    // Give bob read on the entity and he should be able to get the tag
    SecurityRequestContext.setUserId(ALICE.getName());
    authorizer.grant(appId, BOB, Collections.singleton(Action.READ));

    SecurityRequestContext.setUserId(BOB.getName());
    tags = metadataAdmin.getTags(MetadataScope.USER, appId);
    Assert.assertTrue(tags.contains("someTag"));

    // But Alice should be able to remove the tag
    SecurityRequestContext.setUserId(ALICE.getName());
    metadataAdmin.removeTags(appId, "someTag");
    tags = metadataAdmin.getTags(MetadataScope.USER, appId);
    Assert.assertFalse(tags.contains("someTag"));
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
