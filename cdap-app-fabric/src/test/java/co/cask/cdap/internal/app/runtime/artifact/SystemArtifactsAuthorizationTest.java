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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.gateway.handlers.InMemoryAuthorizer;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.AuthorizerInstantiatorService;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

/**
 * Tests for authorization for system artifacts. These tests are not in AuthorizationTest, because we do not want to
 * expose system artifacts capabilities in TestBase. This has to be in its own class because we want to enable
 * authorization in the CDAP that this test starts up.
 */
public class SystemArtifactsAuthorizationTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final String OLD_USER_ID = SecurityRequestContext.getUserId();

  private static ArtifactRepository artifactRepository;
  private static Authorizer authorizer;
  private static InstanceId instance;

  @BeforeClass
  public static void setup() throws Exception {

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, deploymentJar.toURI().getPath());
    Injector injector =  AppFabricTestHelper.getInjector(cConf);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
    AuthorizerInstantiatorService instantiatorService = injector.getInstance(AuthorizerInstantiatorService.class);
    authorizer = instantiatorService.get();
    instance = new InstanceId(cConf.get(Constants.INSTANCE_NAME));
  }

  @Test
  public void testAuthorizationForSystemArtifacts() throws Exception {
    // the system user must be able to add system artifacts
    SecurityRequestContext.setUserId(Principal.SYSTEM.getName());
    artifactRepository.addSystemArtifacts();
    // alice should not be able to refresh system artifacts because she does not have write privileges on the
    // CDAP instance
    SecurityRequestContext.setUserId(ALICE.getName());
    try {
      artifactRepository.addSystemArtifacts();
      Assert.fail("Adding system artifacts should have failed because alice does not have write privileges on " +
                    "the CDAP instance.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant alice write privileges on the CDAP instance
    authorizer.grant(instance, ALICE, Collections.singleton(Action.WRITE));
    Assert.assertEquals(Collections.singleton(new Privilege(instance, Action.WRITE)), authorizer.listPrivileges(ALICE));
    // refreshing system artifacts should succeed now
    artifactRepository.addSystemArtifacts();
    // deleting a system artifact should still fail because alice does not have admin privileges on the CDAP instance
    NamespacedArtifactId systemArtifact = NamespaceId.SYSTEM.artifact("system-artifact", "1.0");
    try {
      artifactRepository.deleteArtifact(systemArtifact.toId());
      Assert.fail("Deleting a system artifact should have failed because alice does not have admin privileges on " +
                    "the CDAP instance.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant alice admin privileges on the CDAP instance
    authorizer.grant(instance, ALICE, Collections.singleton(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.WRITE), new Privilege(instance, Action.ADMIN)),
      authorizer.listPrivileges(ALICE)
    );
    // deleting system artifact should succeed now
    artifactRepository.deleteArtifact(systemArtifact.toId());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    authorizer.revoke(instance);
    Assert.assertEquals(ImmutableSet.<Privilege>of(), authorizer.listPrivileges(ALICE));
    SecurityRequestContext.setUserId(OLD_USER_ID);
  }
}
