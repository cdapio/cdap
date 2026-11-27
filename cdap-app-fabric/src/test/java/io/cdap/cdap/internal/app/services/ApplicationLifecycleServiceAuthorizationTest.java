/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.PluginInstanceDetail;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Authorization tests for {@link ApplicationLifecycleService#getPlugins}.
 */
public class ApplicationLifecycleServiceAuthorizationTest {

  private static final Principal BOB = new Principal("als-bob", Principal.PrincipalType.USER);
  private static final Principal CAROL = new Principal("als-carol", Principal.PrincipalType.USER);

  private static final NamespaceId NAMESPACE = new NamespaceId("alsns");
  private static final String APP1 = "app1";
  private static final String APP2 = "app2";
  private static final String MISSING_APP = "missingApp";

  private static InMemoryAccessController accessController;
  private static AuthenticationContext authenticationContext;

  @BeforeClass
  public static void setup() {
    accessController = new InMemoryAccessController();
    authenticationContext = new AuthenticationTestContext();
    grant(NAMESPACE, BOB, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE, CAROL, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE.app(APP1), CAROL, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE.app(APP2), BOB, EnumSet.of(StandardPermission.GET));
    grant(NAMESPACE.app(MISSING_APP), BOB, EnumSet.of(StandardPermission.GET));
  }

  @After
  public void tearDown() {
    SecurityRequestContext.reset();
  }

  @Test
  public void testGetPluginsRequiresApplicationGetPermission() throws Exception {
    Store store = Mockito.mock(Store.class);
    ApplicationReference app2Ref = NAMESPACE.appReference(APP2);
    ApplicationReference missingAppRef = NAMESPACE.appReference(MISSING_APP);

    Plugin plugin = new Plugin(
        Collections.emptyList(),
        new ArtifactId("google-cloud", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
        PluginClass.builder()
            .setName("GCS")
            .setType("batchsource")
            .setDescription("GCS source")
            .setClassName("io.cdap.plugin.GCS")
            .build(),
        PluginProperties.builder().add("serviceAccountKey", "secret").build());
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(appSpec.getPlugins()).thenReturn(ImmutableMap.of("p1", plugin));
    Mockito.when(store.getLatest(app2Ref)).thenReturn(new ApplicationMeta(APP2, appSpec, null));

    ApplicationLifecycleService service = new ApplicationLifecycleService(
        CConfiguration.create(), store, null, null, null, null, null, null, null, null,
        accessController, authenticationContext, null, null, null,
        new NoOpMetricsCollectionService(), Mockito.mock(RemoteClientFactory.class), null);

    // Principal with namespace GET and GET on app1 (not app2 or missingApp) is rejected before
    // consulting the store, preventing both property disclosure and app existence probing.
    AuthenticationTestContext.actAsPrincipal(CAROL);
    Assert.assertThrows(UnauthorizedException.class, () -> service.getPlugins(app2Ref));
    Assert.assertThrows(UnauthorizedException.class, () -> service.getPlugins(missingAppRef));
    Mockito.verifyNoInteractions(store);

    // Principal with GET on app2 and missingApp can read app2's plugins and sees 404 on missingApp.
    AuthenticationTestContext.actAsPrincipal(BOB);
    List<PluginInstanceDetail> plugins = service.getPlugins(app2Ref);
    Assert.assertEquals(1, plugins.size());
    Assert.assertEquals("p1", plugins.get(0).getId());
    Assert.assertEquals("GCS", plugins.get(0).getName());
    Assert.assertEquals("secret",
        plugins.get(0).getSpec().getProperties().getProperties().get("serviceAccountKey"));

    Assert.assertThrows(ApplicationNotFoundException.class,
        () -> service.getPlugins(missingAppRef));
  }

  private static void grant(EntityId entity, Principal principal,
      Set<? extends Permission> permissions) {
    accessController.grant(Authorizable.fromEntityId(entity), principal, permissions);
  }
}
