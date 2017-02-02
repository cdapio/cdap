/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.logging.AuditLogContent;
import co.cask.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.google.common.collect.ImmutableList;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * To test the {@link RouterAuditLookUp} scans all the handler classes that needs to be audit logged with more info.
 */
public class RouterAuditLookUpTest {

  private static final RouterAuditLookUp AUDIT_LOOK_UP = RouterAuditLookUp.getAuditLookUp();
  private static final List<String> EMPTY_HEADERS = ImmutableList.of();
  private static final AuditLogContent DEFAULT_AUDIT = new AuditLogContent(HttpMethod.PUT, true, false, EMPTY_HEADERS);

  @Test
  public void testDataFabricEndpoints() throws Exception {
    // endpoints from DatasetInstanceHandler
    assertContent("/v3/namespaces/default/data/datasets/myDataset", DEFAULT_AUDIT);
    // endpoints from DatasetTypeHandler
    assertContent("/v3/namespaces/default/data/modules/myModule",
                  new AuditLogContent(HttpMethod.PUT, false, false, ImmutableList.of("X-Class-Name")));
    // endpoints from StreamHandler
    assertContent("/v3/namespaces/default/streams/myStream", DEFAULT_AUDIT);
    // endpoints from StreamViewHttpHandler
    assertContent("/v3/namespaces/default/streams/foo/views/myView", DEFAULT_AUDIT);
  }

  @Test
  public void testAppFabricEndpoints() throws Exception {
    // endpoints from AppLifecycleHttpHandler
    assertContent("/v3/namespaces/default/apps/myApp", DEFAULT_AUDIT);
    assertContent("/v3/namespaces/default/apps",
                  new AuditLogContent(HttpMethod.POST, false, true,
                                      ImmutableList.of(AbstractAppFabricHttpHandler.ARCHIVE_NAME_HEADER,
                                                       AbstractAppFabricHttpHandler.APP_CONFIG_HEADER,
                                                       AbstractAppFabricHttpHandler.PRINCIPAL_HEADER)));
    // endpoints from ArtifactHttpHandler
    assertContent("/v3/namespaces/default/artifacts/myArtifact/versions/1.0/properties", DEFAULT_AUDIT);
    assertContent("/v3/namespaces/default/artifacts/myArtifact",
                  new AuditLogContent(HttpMethod.POST, false, false,
                                      ImmutableList.of("Artifact-Version", "Artifact-Extends", "Artifact-Plugins")));
    // endpoints from AuthorizationHandler
    assertContent("/v3/security/authorization/privileges/grant",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
    // endpoints from ConsoleSettingsHttpHandler
    assertContent("/v3/configuration/user/", DEFAULT_AUDIT);
    // endpoints from DashboardHttpHandler
    assertContent("/v3/namespaces/default/configuration/dashboards",
                  new AuditLogContent(HttpMethod.POST, true, true, EMPTY_HEADERS));
    // endpoints from MetadataHttpHandler
    assertContent("/v3/namespaces/default/apps/app1/metadata/properties",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
    // endpoints from MonitorHttpHandler
    assertContent("/v3/system/services/appfabric/instances", DEFAULT_AUDIT);
    // endpoints from NamespaceHttpHandler
    assertContent("/v3/namespaces/default", DEFAULT_AUDIT);
    // endpoints from NotificationFeedHttpHandler
    assertContent("/v3/namespaces/default/feeds/categories/feed1/names/myFeed", DEFAULT_AUDIT);
    // endpoints from PreferencesHttpHandler
    assertContent("/v3/preferences", DEFAULT_AUDIT);
    // endpoints from ProgramLifecycleHttpHandler
    assertContent("/v3/namespaces/default/stop", new AuditLogContent(HttpMethod.POST, true, true, EMPTY_HEADERS));
    // endpoints from RouteConfigHttpHandler
    assertContent("/v3/namespaces/default/apps/myApp/services/myService/routeconfig", DEFAULT_AUDIT);
    // endpoints from SecureStoreHandler
    assertContent("/v3/namespaces/default/securekeys/myKey", DEFAULT_AUDIT);
    // endpoints from TransactionHttpHandler
    assertContent("/v3/transactions/invalid/remove/until",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
  }

  @Test
  public void testExploreEndpoints() throws Exception {
    // endpoints from ExploreExecutorHttpHandler
    assertContent("/v3/namespaces/default/data/explore/datasets/myDataset/update",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
    // endpoints from NamespacedExploreMetadataHttpHandler
    assertContent("/v3/namespaces/default/data/explore/jdbc/tables",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
    // endpoints from NamespacedQueryExecutorHttpHandler
    assertContent("/v3/namespaces/default/data/explore/queries",
                  new AuditLogContent(HttpMethod.POST, true, false, EMPTY_HEADERS));
  }

  private void assertContent(String path, AuditLogContent expected) throws Exception {
    Assert.assertEquals(expected, AUDIT_LOOK_UP.getAuditLogContent(path, expected.getHttpMethod()));
  }
}
