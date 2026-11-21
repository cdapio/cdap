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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.data2.dataset2.lib.external.ExternalDataset;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;

/**
 * Unit tests for {@link RuntimeCallbackAllowlist}.
 */
public class RuntimeCallbackAllowlistTest {

  private static final Gson GSON = new Gson();
  private static final String STUDIO_SERVICE = "svc.system.pipeline.studio";
  private static final String DATAPREP_SERVICE = "svc.system.dataprep.service";
  private static final String DATAPREP_CONFIG_PATH =
      "/v3/namespaces/system/apps/dataprep/services/service/methods/config";

  private ProgramRunId tokenRunId;
  private RuntimeCallbackAllowlist allowlist;

  @Before
  public void setUp() {
    tokenRunId = new NamespaceId("runns").app("myApp", "1.0").workflow("DataPipelineWorkflow")
        .run(RunIds.generate());
    allowlist = new RuntimeCallbackAllowlist(CConfiguration.create());
  }

  private RuntimeCallbackAllowlist.MatchResult match(String service, String method,
      String servicePath) {
    return match(service, method, servicePath, null);
  }

  private RuntimeCallbackAllowlist.MatchResult match(String service, String method,
      String servicePath, @Nullable RuntimeScopes scopes) {
    return allowlist.match(servicePath, service, method, tokenRunId, scopes);
  }

  @Test
  public void testAllBundledRulesMatch() {
    // 1. secure.store.service GET secret value
    RuntimeCallbackAllowlist.MatchResult m1 = match(Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/my-secret");
    Assert.assertTrue(m1.isAllowed());
    Assert.assertFalse(m1.requiresBodyCheck());
    Assert.assertEquals("/v3/namespaces/{namespace}/securekeys/{name}", m1.getLogPath());
    Assert.assertEquals("my-secret", m1.getCapture("name"));

    // 2. secure.store.service GET secret metadata
    RuntimeCallbackAllowlist.MatchResult m2 = match(Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/my-secret/metadata");
    Assert.assertTrue(m2.isAllowed());
    Assert.assertFalse(m2.requiresBodyCheck());
    Assert.assertEquals("/v3/namespaces/{namespace}/securekeys/{name}/metadata", m2.getLogPath());

    // 3. appfabric GET artifact download (RUN and system)
    RuntimeCallbackAllowlist.MatchResult m3Run = match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3Internal/namespaces/runns/artifacts/my-art/versions/1.0.0/download?scope=user");
    Assert.assertTrue(m3Run.isAllowed());
    RuntimeCallbackAllowlist.MatchResult m3Sys = match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3Internal/namespaces/system/artifacts/core-plugins/versions/2.0/download?scope=system");
    Assert.assertTrue(m3Sys.isAllowed());

    // 4. appfabric GET plugin metadata
    RuntimeCallbackAllowlist.MatchResult m4 = match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3/namespaces/runns/artifacts/cdap-data-pipeline/versions/6.11.0/extensions/batchsource"
            + "/plugins/File?scope=system");
    Assert.assertTrue(m4.isAllowed());

    // 5. appfabric GET / PUT / DELETE app state
    for (String method : Arrays.asList("GET", "PUT", "DELETE")) {
      RuntimeCallbackAllowlist.MatchResult m5 = match(Constants.Service.APP_FABRIC_HTTP, method,
          "/v3Internal/namespaces/runns/apps/myApp/states/checkpoint");
      Assert.assertTrue("Expected " + method + " app state allowed", m5.isAllowed());
      Assert.assertFalse(m5.requiresBodyCheck());
    }

    // 6. appfabric GET namespace
    RuntimeCallbackAllowlist.MatchResult m6 = match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3/namespaces/runns");
    Assert.assertTrue(m6.isAllowed());

    // 7. studio GET connection
    RuntimeCallbackAllowlist.MatchResult m7 = match(STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/my+conn");
    Assert.assertTrue(m7.isAllowed());
    Assert.assertEquals("my+conn", m7.getCapture("connection"));

    // 8. studio GET oauth credential
    RuntimeCallbackAllowlist.MatchResult m8 = match(STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/github"
            + "/credential/cred1");
    Assert.assertTrue(m8.isAllowed());
    Assert.assertEquals("github", m8.getCapture("provider"));
    Assert.assertEquals("cred1", m8.getCapture("credential"));

    // 9. dataprep GET directive config
    RuntimeCallbackAllowlist.MatchResult dataprep =
        match(DATAPREP_SERVICE, "GET", DATAPREP_CONFIG_PATH);
    Assert.assertTrue(dataprep.isAllowed());
    Assert.assertFalse(dataprep.requiresBodyCheck());
    Assert.assertEquals(DATAPREP_CONFIG_PATH, dataprep.getLogPath());

    // 10. dataset.service GET dataset
    RuntimeCallbackAllowlist.MatchResult m10 = match(Constants.Service.DATASET_MANAGER, "GET",
        "/v3/namespaces/runns/data/datasets/orders");
    Assert.assertTrue(m10.isAllowed());
    Assert.assertFalse(m10.requiresBodyCheck());

    // 11. dataset.service PUT dataset (requires body check)
    RuntimeCallbackAllowlist.MatchResult m11 = match(Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/orders");
    Assert.assertTrue(m11.isAllowed());
    Assert.assertTrue(m11.requiresBodyCheck());

    // 12. dataset.service DELETE workflow-local dataset
    RuntimeCallbackAllowlist.MatchResult m12 = match(Constants.Service.DATASET_MANAGER, "DELETE",
        "/v3/namespaces/runns/data/datasets/conn-1." + tokenRunId.getRun());
    Assert.assertTrue(m12.isAllowed());
    Assert.assertFalse(m12.requiresBodyCheck());
  }

  @Test
  public void testNeighborsDenied() {
    // Wrong method on every allowlisted path
    for (String method : Arrays.asList("PUT", "POST", "DELETE")) {
      Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, method,
          "/v3/namespaces/runns/securekeys/my-secret").isAllowed());
      Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, method,
          "/v3/namespaces/runns/securekeys/my-secret/metadata").isAllowed());
      Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, method,
          "/v3Internal/namespaces/runns/artifacts/my-art/versions/1.0/download").isAllowed());
      Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, method,
          "/v3/namespaces/runns/artifacts/pipe/versions/1.0/extensions/t/plugins/p").isAllowed());
      Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, method,
          "/v3/namespaces/runns").isAllowed());
      Assert.assertFalse(match(STUDIO_SERVICE, method,
          "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
              + "/connections/myconn").isAllowed());
      Assert.assertFalse(match(STUDIO_SERVICE, method,
          "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/p"
              + "/credential/c").isAllowed());
      Assert.assertFalse(match(DATAPREP_SERVICE, method, DATAPREP_CONFIG_PATH).isAllowed());
    }
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "POST",
        "/v3Internal/namespaces/runns/apps/myApp/states/checkpoint").isAllowed());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "POST",
        "/v3/namespaces/runns/data/datasets/orders").isAllowed());

    // Wrong namespace (both otherns and system where only RUN is allowed) across all bundled rules
    RuntimeCallbackAllowlist.MatchResult wrongNsSecret = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/otherns/securekeys/secret1");
    Assert.assertFalse(wrongNsSecret.isAllowed());
    Assert.assertEquals("/v3/namespaces/{namespace}/securekeys/{name}", wrongNsSecret.getLogPath());

    for (String wrongNs : Arrays.asList("otherns", NamespaceId.SYSTEM.getNamespace())) {
      Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, "GET",
          "/v3/namespaces/" + wrongNs + "/securekeys/secret1").isAllowed());
      Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, "GET",
          "/v3/namespaces/" + wrongNs + "/securekeys/secret1/metadata").isAllowed());
      Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
          "/v3/namespaces/" + wrongNs + "/artifacts/a/versions/1.0/extensions/t/plugins/p")
          .isAllowed());
      for (String method : Arrays.asList("GET", "PUT", "DELETE")) {
        Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, method,
            "/v3Internal/namespaces/" + wrongNs + "/apps/myApp/states/k").isAllowed());
      }
      Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
          "/v3/namespaces/" + wrongNs).isAllowed());
      Assert.assertFalse(match(STUDIO_SERVICE, "GET",
          "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/" + wrongNs
              + "/connections/c").isAllowed());
      Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "GET",
          "/v3/namespaces/" + wrongNs + "/data/datasets/ds").isAllowed());
      Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "PUT",
          "/v3/namespaces/" + wrongNs + "/data/datasets/ds").isAllowed());
      Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "DELETE",
          "/v3/namespaces/" + wrongNs + "/data/datasets/conn-1." + tokenRunId.getRun())
          .isAllowed());
    }
    // Artifact download allows system, but denies otherns
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3Internal/namespaces/otherns/artifacts/a/versions/1.0/download").isAllowed());

    // Wrong application for app state
    RuntimeCallbackAllowlist.MatchResult wrongApp = match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3Internal/namespaces/runns/apps/otherApp/states/k");
    Assert.assertFalse(wrongApp.isAllowed());
    Assert.assertEquals("/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}",
        wrongApp.getLogPath());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "PUT",
        "/v3Internal/namespaces/runns/apps/otherApp/states/k").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "DELETE",
        "/v3Internal/namespaces/runns/apps/otherApp/states/k").isAllowed());

    // List endpoints and write endpoints
    Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys").isAllowed());
    Assert.assertFalse(match(Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "DELETE",
        "/v3Internal/namespaces/runns/apps/myApp/states").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "DELETE",
        "/v3Internal/namespaces/runns/apps/myApp/states/").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3/namespaces").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3/namespaces/runns/apps").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "GET",
        "/v3/namespaces/runns/artifacts").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "POST",
        "/v3/namespaces/runns/artifacts/my-art").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "POST",
        "/v3/namespaces/runns/apps/myApp/workflows/wf/start").isAllowed());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/ds/properties").isAllowed());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "POST",
        "/v3/namespaces/runns/data/datasets/ds/admin/create").isAllowed());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "GET",
        "/v3/namespaces/runns/data/datasets").isAllowed());
    Assert.assertFalse(match(STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections").isAllowed());
    Assert.assertFalse(match(STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/p")
        .isAllowed());
    for (String path : Arrays.asList(
        DATAPREP_CONFIG_PATH + "/extra",
        "/v3/namespaces/system/apps/dataprep/services/service/methods/info",
        "/v3/namespaces/system/apps/dataprep/services/service/methods/contexts/runns/workspaces",
        "/v3/namespaces/runns/apps/dataprep/services/service/methods/config",
        "/v3/namespaces/system/apps/other/services/service/methods/config")) {
      Assert.assertFalse(path, match(DATAPREP_SERVICE, "GET", path).isAllowed());
    }
    Assert.assertFalse(match(STUDIO_SERVICE, "GET", DATAPREP_CONFIG_PATH).isAllowed());

    // Unlisted service: fallback logPath is decoded and has query string removed
    RuntimeCallbackAllowlist.MatchResult unlisted = match("metadata.service", "GET",
        "/v3/namespaces/runns/my%20meta?key=val");
    Assert.assertFalse(unlisted.isAllowed());
    Assert.assertEquals("/v3/namespaces/runns/my meta", unlisted.getLogPath());

    // Dataset DELETE for foreign dataset, empty prefix before run id, or another run id
    RuntimeCallbackAllowlist.MatchResult foreignDelete = match(
        Constants.Service.DATASET_MANAGER, "DELETE",
        "/v3/namespaces/runns/data/datasets/other-dataset");
    Assert.assertFalse(foreignDelete.isAllowed());
    Assert.assertEquals("/v3/namespaces/{namespace}/data/datasets/{name}",
        foreignDelete.getLogPath());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "DELETE",
        "/v3/namespaces/runns/data/datasets/." + tokenRunId.getRun()).isAllowed());
    Assert.assertFalse(match(Constants.Service.DATASET_MANAGER, "DELETE",
        "/v3/namespaces/runns/data/datasets/conn-1." + RunIds.generate().getId()).isAllowed());
  }

  @Test
  public void testPathRuleAcceptedAndRejectedExamples() {
    String svc = Constants.Service.SECURE_STORE_SERVICE;

    // Accepted service paths (query parameters, single trailing slash, percent-encoded segments)
    Assert.assertTrue(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key?foo=bar").isAllowed());
    Assert.assertTrue(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key/").isAllowed());
    Assert.assertTrue(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key/?foo=bar").isAllowed());
    RuntimeCallbackAllowlist.MatchResult encoded = match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my%20key+1%2B2");
    Assert.assertTrue(encoded.isAllowed());
    Assert.assertEquals("my key+1+2", encoded.getCapture("name"));

    // Rejected service paths
    // 1. %2e%2e (encoded dot-dot) and literal .. / . / %2e
    Assert.assertFalse(match(svc, "GET", "/v3/namespaces/runns/securekeys/%2e%2e").isAllowed());
    Assert.assertFalse(match(svc, "GET", "/v3/namespaces/runns/securekeys/..").isAllowed());
    Assert.assertFalse(match(svc, "GET", "/v3/namespaces/runns/securekeys/.").isAllowed());
    Assert.assertFalse(match(svc, "GET", "/v3/namespaces/runns/securekeys/%2e").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/foo/%2e%2e/bar").isAllowed());

    // 2. %2F / %2f (encoded slash inside segment, after leading '/', and without leading '/')
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/a%2Fb").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/%2fv3/namespaces/runns/securekeys/my-key").isAllowed());
    Assert.assertFalse(allowlist.match(
        "%2fv3/namespaces/runns/securekeys/my-key", svc, "GET", tokenRunId, null).isAllowed());
    Assert.assertFalse(allowlist.match("", svc, "GET", tokenRunId, null).isAllowed());
    Assert.assertFalse(allowlist.match(
        "extra/v3/namespaces/runns/securekeys/my-key", svc, "GET", tokenRunId, null).isAllowed());

    // 3. // and /// (leading or internal empty segments)
    Assert.assertFalse(match(svc, "GET",
        "//v3/namespaces/runns/securekeys/my-key").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "///v3/namespaces/runns/securekeys/my-key").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces//runns/securekeys/my-key").isAllowed());

    // 4. backslash (raw and encoded %5c / %5C)
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/a\\b").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/a%5cb").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/a%5Cb").isAllowed());

    // 5. raw space
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my key").isAllowed());

    // 6. bad percent-escape (must return denied, not throw)
    RuntimeCallbackAllowlist.MatchResult badEscape = match(svc, "GET",
        "/v3/namespaces/runns/securekeys/bad%ZZ?secret=1");
    Assert.assertFalse(badEscape.isAllowed());
    Assert.assertEquals("/v3/namespaces/runns/securekeys/bad%ZZ", badEscape.getLogPath());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/bad%2").isAllowed());

    // 7. two trailing slashes
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key//").isAllowed());

    // 8. trailing slash on parent collection (.../states/ and .../securekeys/)
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/").isAllowed());
    Assert.assertFalse(match(Constants.Service.APP_FABRIC_HTTP, "DELETE",
        "/v3Internal/namespaces/runns/apps/myApp/states/").isAllowed());

    // 9. fragment
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key#frag").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my-key#").isAllowed());

    // 10. control characters (C0 and DEL)
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my%00key").isAllowed());
    Assert.assertFalse(match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my%7fkey").isAllowed());
    RuntimeCallbackAllowlist.MatchResult ctrlLog = match(svc, "GET",
        "/v3/namespaces/runns/securekeys/my%0akey");
    Assert.assertFalse(ctrlLog.isAllowed());
    Assert.assertFalse(ctrlLog.getLogPath().contains("\n"));
  }

  @Test
  public void testNettyHttpServiceRoutingParityForAcceptedExamples() throws Exception {
    NettyHttpService service = NettyHttpService.builder("allowlist-parity")
        .setHost(InetAddress.getLocalHost().getCanonicalHostName())
        .setHttpHandlers(new ParityHandler())
        .build();
    service.start();
    try {
      Discoverable discoverable = URIScheme.createDiscoverable("allowlist-parity", service);

      List<ParityCase> cases = Arrays.asList(
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my-secret",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "my-secret")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my-secret/",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "my-secret")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my-secret/?foo=bar",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "my-secret")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my%20secret+1%2B2?ignore=1",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "my secret+1+2")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my%252Fsecret",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "my%2Fsecret")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/a;b=1",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "a;b=1")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/file%2etxt",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "file.txt")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/a..b",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "a..b")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/caf%C3%A9",
              "/v3/namespaces/{namespace}/securekeys/{name}",
              mapOf("namespace", "runns", "name", "café")),
          new ParityCase(Constants.Service.SECURE_STORE_SERVICE, "GET",
              "/v3/namespaces/runns/securekeys/my-secret/metadata/",
              "/v3/namespaces/{namespace}/securekeys/{name}/metadata",
              mapOf("namespace", "runns", "name", "my-secret")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "GET",
              "/v3Internal/namespaces/system/artifacts/core-plugins/versions/1.2.3/download?scope=system",
              "/v3Internal/namespaces/{namespace}/artifacts/{artifact}/versions/{version}/download",
              mapOf("namespace", "system", "artifact", "core-plugins", "version", "1.2.3")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "GET",
              "/v3/namespaces/runns/artifacts/pipe/versions/6.11/extensions/batchsource/plugins/File?scope=user",
              "/v3/namespaces/{namespace}/artifacts/{artifact}/versions/{version}/extensions/{type}/plugins/{plugin}",
              mapOf("namespace", "runns", "artifact", "pipe", "version", "6.11",
                  "type", "batchsource", "plugin", "File")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "GET",
              "/v3Internal/namespaces/runns/apps/myApp/states/state%20key",
              "/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}",
              mapOf("namespace", "runns", "app", "myApp", "key", "state key")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "PUT",
              "/v3Internal/namespaces/runns/apps/myApp/states/state-key/",
              "/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}",
              mapOf("namespace", "runns", "app", "myApp", "key", "state-key")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "DELETE",
              "/v3Internal/namespaces/runns/apps/myApp/states/state-key",
              "/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}",
              mapOf("namespace", "runns", "app", "myApp", "key", "state-key")),
          new ParityCase(Constants.Service.APP_FABRIC_HTTP, "GET",
              "/v3/namespaces/runns/",
              "/v3/namespaces/{namespace}",
              mapOf("namespace", "runns")),
          new ParityCase(STUDIO_SERVICE, "GET",
              "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
                  + "/connections/my+conn%20name",
              "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}"
                  + "/connections/{connection}",
              mapOf("namespace", "runns", "connection", "my+conn name")),
          new ParityCase(STUDIO_SERVICE, "GET",
              "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/prov-1"
                  + "/credential/cred%201/",
              "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/{provider}"
                  + "/credential/{credential}",
              mapOf("provider", "prov-1", "credential", "cred 1")),
          new ParityCase(DATAPREP_SERVICE, "GET",
              DATAPREP_CONFIG_PATH,
              DATAPREP_CONFIG_PATH,
              Collections.emptyMap()),
          new ParityCase(Constants.Service.DATASET_MANAGER, "GET",
              "/v3/namespaces/runns/data/datasets/ds%201",
              "/v3/namespaces/{namespace}/data/datasets/{name}",
              mapOf("namespace", "runns", "name", "ds 1")),
          new ParityCase(Constants.Service.DATASET_MANAGER, "PUT",
              "/v3/namespaces/runns/data/datasets/conn-1." + tokenRunId.getRun(),
              "/v3/namespaces/{namespace}/data/datasets/{name}",
              mapOf("namespace", "runns", "name", "conn-1." + tokenRunId.getRun())),
          new ParityCase(Constants.Service.DATASET_MANAGER, "DELETE",
              "/v3/namespaces/runns/data/datasets/conn-1." + tokenRunId.getRun() + "/",
              "/v3/namespaces/{namespace}/data/datasets/{name}",
              mapOf("namespace", "runns", "name", "conn-1." + tokenRunId.getRun()))
      );

      for (ParityCase tc : cases) {
        RuntimeCallbackAllowlist.MatchResult allowlistResult = match(
            tc.service, tc.method, tc.servicePath);
        Assert.assertTrue("Allowlist should accept " + tc.servicePath,
            allowlistResult.isAllowed());
        Assert.assertEquals(tc.expectedTemplate, allowlistResult.getLogPath());
        for (Map.Entry<String, String> entry : tc.expectedCaptures.entrySet()) {
          Assert.assertEquals("Allowlist capture mismatch for " + tc.servicePath,
              entry.getValue(), allowlistResult.getCapture(entry.getKey()));
        }

        // Construct downstream URL the same way AbstractServiceRoutingHandler.openConnection does.
        URL url = URIScheme.createURI(discoverable, "%s", tc.servicePath).toURL();
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
          conn.setRequestMethod(tc.method);
          if ("PUT".equals(tc.method)) {
            conn.setDoOutput(true);
            conn.getOutputStream().close();
          }
          Assert.assertEquals("NettyHttpService status for " + tc.servicePath,
              200, conn.getResponseCode());
          StringBuilder body = new StringBuilder();
          try (BufferedReader reader = new BufferedReader(
              new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
              body.append(line);
            }
          }
          ParityResponse resp = GSON.fromJson(body.toString(), ParityResponse.class);
          Assert.assertEquals("Wire URI altered for " + tc.servicePath, tc.servicePath, resp.uri);
          Assert.assertEquals("Template mismatch for " + tc.servicePath,
              tc.expectedTemplate, resp.template);
          Assert.assertEquals("NettyHttpService capture mismatch for " + tc.servicePath,
              tc.expectedCaptures, resp.captures);
        } finally {
          conn.disconnect();
        }
      }
    } finally {
      service.stop();
    }
  }

  @Test
  public void testExtraRulesAndInvalidConfig() {
    // Default config, explicit empty string, and empty array do not log a WARN
    Logger defaultLogger = Mockito.mock(Logger.class);
    new RuntimeCallbackAllowlist(CConfiguration.create(), defaultLogger);
    Mockito.verifyNoInteractions(defaultLogger);

    CConfiguration emptyConf = CConfiguration.create();
    emptyConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES, "");
    Logger emptyLogger = Mockito.mock(Logger.class);
    new RuntimeCallbackAllowlist(emptyConf, emptyLogger);
    Mockito.verifyNoInteractions(emptyLogger);

    CConfiguration emptyArrayConf = CConfiguration.create();
    emptyArrayConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES, "[]");
    Logger emptyArrayLogger = Mockito.mock(Logger.class);
    new RuntimeCallbackAllowlist(emptyArrayConf, emptyArrayLogger);
    Mockito.verifyNoInteractions(emptyArrayLogger);

    // Extra rule adds a path and logs a WARN; also verifies an unconditional extra rule on
    // dataset PUT is not shadowed by the bundled DATASET_PUT rule.
    CConfiguration extraConf = CConfiguration.create();
    extraConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES,
        "[{\"service\":\"custom.service\",\"methods\":[\"GET\"],"
            + "\"pathTemplate\":\"/v3/namespaces/{namespace}/items/{item}\","
            + "\"namespaceRule\":\"RUN\"},"
            + "{\"service\":\"dataset.service\",\"methods\":[\"PUT\"],"
            + "\"pathTemplate\":\"/v3/namespaces/{namespace}/data/datasets/special-table\","
            + "\"namespaceRule\":\"RUN\"}]");
    Logger warnLogger = Mockito.mock(Logger.class);
    RuntimeCallbackAllowlist withExtra = new RuntimeCallbackAllowlist(extraConf, warnLogger);
    Mockito.verify(warnLogger).warn(Mockito.anyString(), Mockito.eq(2),
        Mockito.eq(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES));

    Assert.assertTrue(withExtra.match("/v3/namespaces/runns/items/x",
        "custom.service", "GET", tokenRunId, null).isAllowed());
    Assert.assertFalse(withExtra.match("/v3/namespaces/other/items/x",
        "custom.service", "GET", tokenRunId, null).isAllowed());

    // Extra dataset PUT rule with bodyRule=NONE overrides bundled DATASET_PUT for special-table
    RuntimeCallbackAllowlist.MatchResult overriddenPut = withExtra.match(
        "/v3/namespaces/runns/data/datasets/special-table",
        Constants.Service.DATASET_MANAGER, "PUT", tokenRunId, null);
    Assert.assertTrue(overriddenPut.isAllowed());
    Assert.assertFalse(overriddenPut.requiresBodyCheck());
    // Other dataset names still require body check from bundled rule
    RuntimeCallbackAllowlist.MatchResult normalPut = withExtra.match(
        "/v3/namespaces/runns/data/datasets/other-table",
        Constants.Service.DATASET_MANAGER, "PUT", tokenRunId, null);
    Assert.assertTrue(normalPut.isAllowed());
    Assert.assertTrue(normalPut.requiresBodyCheck());

    // Bundled rules remain intact
    Assert.assertTrue(withExtra.match("/v3/namespaces/runns/securekeys/k",
        Constants.Service.SECURE_STORE_SERVICE, "GET", tokenRunId, null).isAllowed());

    // Malformed, non-array, or non-object rule JSON is logged and ignored
    assertInvalidExtraRulesIgnored("[not-json", "Invalid JSON in runtime callback allowlist");
    assertInvalidExtraRulesIgnored("{}", "must be a JSON array");
    assertInvalidExtraRulesIgnored("[123]", "must be a JSON object");
    assertInvalidExtraRulesIgnored("[null]", "must be a JSON object");

    // Recursive variable substitution is logged and ignored
    assertInvalidExtraRulesIgnored("${app.program.runtime.token.allowlist.extra.rules}",
        "Variable substitution depth");

    // One valid rule plus one invalid rule ignores all extra rules (all-or-nothing)
    RuntimeCallbackAllowlist mixedInvalid = assertInvalidExtraRulesIgnored(
        "[{\"service\":\"custom.service\",\"methods\":[\"GET\"],"
            + "\"pathTemplate\":\"/v3/namespaces/{namespace}/items/{item}\","
            + "\"namespaceRule\":\"RUN\"},"
            + "{\"service\":\"svc\",\"methods\":[\"PATCH\"],\"pathTemplate\":\"/v3/ping\"}]",
        "Unsupported method 'PATCH'");
    Assert.assertFalse(mixedInvalid.match("/v3/namespaces/runns/items/x",
        "custom.service", "GET", tokenRunId, null).isAllowed());

    // Missing, non-string, or empty service / pathTemplate is logged and ignored
    assertInvalidExtraRulesIgnored("[{\"methods\":[\"GET\"],\"pathTemplate\":\"/v3/ping\"}]",
        "Missing or non-string field 'service'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":123,\"methods\":[\"GET\"],\"pathTemplate\":\"/v3/ping\"}]",
        "Missing or non-string field 'service'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"\",\"methods\":[\"GET\"],\"pathTemplate\":\"/v3/ping\"}]",
        "Empty field 'service'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"]}]",
        "Missing or non-string field 'pathTemplate'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"svc\",\"methods\":[\"GET\"],\"pathTemplate\":123}]",
        "Missing or non-string field 'pathTemplate'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"svc\",\"methods\":[\"GET\"],\"pathTemplate\":\"\"}]",
        "Empty field 'pathTemplate'");

    // Unknown enum value is logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/namespaces/{namespace}/x\",\"namespaceRule\":\"ALL\"}]",
        "Unknown namespaceRule value 'ALL'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/apps/{app}/x\",\"applicationRule\":\"ANY\"}]",
        "Unknown applicationRule value 'ANY'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PUT\"],"
        + "\"pathTemplate\":\"/v3/data/{name}\",\"bodyRule\":\"UNKNOWN_BODY\"}]",
        "Unknown bodyRule value 'UNKNOWN_BODY'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/secrets/{name}\",\"scopeKind\":\"UNKNOWN_SCOPE\"}]",
        "Unknown scopeKind value 'UNKNOWN_SCOPE'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/secrets/{name}\",\"scopeKind\":null}]",
        "Non-string enum field 'scopeKind'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/secrets/{name}\",\"scopeKind\":123}]",
        "Non-string enum field 'scopeKind'");
    // Combining bodyRule and scopeKind on one rule is logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PUT\"],"
        + "\"pathTemplate\":\"/v3/data/{name}\",\"bodyRule\":\"DATASET_PUT\","
        + "\"scopeKind\":\"SECRET\"}]",
        "cannot combine bodyRule=DATASET_PUT with scopeKind=SECRET");

    // Unknown JSON field is logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/ping\",\"unexpectedField\":\"val\"}]",
        "Unknown field 'unexpectedField'");

    // Missing required capture placeholder is logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/ping\",\"namespaceRule\":\"RUN\"}]",
        "requires {namespace} in pathTemplate");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/ping\",\"applicationRule\":\"RUN\"}]",
        "requires {app} in pathTemplate");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PUT\"],"
        + "\"pathTemplate\":\"/v3/data/fixed\",\"bodyRule\":\"DATASET_PUT\"}]",
        "requires {name} in pathTemplate");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"DELETE\"],"
        + "\"pathTemplate\":\"/v3/data/fixed\",\"bodyRule\":\"DATASET_DELETE\"}]",
        "requires {name} in pathTemplate");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/secrets/fixed\",\"scopeKind\":\"SECRET\"}]",
        "requires [name] in pathTemplate for scopeKind=SECRET");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/conns/fixed\",\"scopeKind\":\"CONNECTION\"}]",
        "requires [connection] in pathTemplate for scopeKind=CONNECTION");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/oauth/{provider}/fixed\",\"scopeKind\":\"OAUTH\"}]",
        "requires [provider, credential] in pathTemplate for scopeKind=OAUTH");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/oauth/fixed/{credential}\",\"scopeKind\":\"OAUTH\"}]",
        "requires [provider, credential] in pathTemplate for scopeKind=OAUTH");

    // Omitting or setting null for namespaceRule / applicationRule when captured is ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/namespaces/{namespace}/x\"}]",
        "captures {namespace} and must explicitly specify namespaceRule");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/namespaces/{namespace}/x\",\"namespaceRule\":null}]",
        "Non-string enum field 'namespaceRule'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/apps/{app}/x\"}]",
        "captures {app} and must explicitly specify applicationRule");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/apps/{app}/x\",\"applicationRule\":null}]",
        "Non-string enum field 'applicationRule'");

    // Missing, non-array, empty, or non-string methods are logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"pathTemplate\":\"/v3/ping\"}]",
        "Missing or non-array field 'methods'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"svc\",\"methods\":\"GET\",\"pathTemplate\":\"/v3/ping\"}]",
        "Missing or non-array field 'methods'");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"svc\",\"methods\":[],\"pathTemplate\":\"/v3/ping\"}]",
        "Empty 'methods' array");
    assertInvalidExtraRulesIgnored(
        "[{\"service\":\"svc\",\"methods\":[123],\"pathTemplate\":\"/v3/ping\"}]",
        "Non-string method");

    // Incompatible bodyRule / HTTP method combinations and unsupported HTTP methods are ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/data/{name}\",\"bodyRule\":\"DATASET_PUT\"}]",
        "bodyRule=DATASET_PUT cannot be paired with method 'GET'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PUT\",\"DELETE\"],"
        + "\"pathTemplate\":\"/v3/data/{name}\",\"bodyRule\":\"DATASET_PUT\"}]",
        "bodyRule=DATASET_PUT cannot be paired with method 'DELETE'");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PUT\"],"
        + "\"pathTemplate\":\"/v3/data/{name}\",\"bodyRule\":\"DATASET_DELETE\"}]",
        "bodyRule=DATASET_DELETE can only be paired with DELETE");
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"PATCH\"],"
        + "\"pathTemplate\":\"/v3/ping\"}]",
        "Unsupported method 'PATCH'");

    // Duplicate capture and malformed pathTemplate forms are logged and ignored
    assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
        + "\"pathTemplate\":\"/v3/namespaces/{namespace}/copy/{namespace}\","
        + "\"namespaceRule\":\"RUN\"}]",
        "Duplicate capture '{namespace}'");
    for (String badTemplate : Arrays.asList("/", "/v3/ping/", "v3/ping")) {
      assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
          + "\"pathTemplate\":\"" + badTemplate + "\"}]",
          "Invalid pathTemplate");
    }
    for (String badSegmentTemplate : Arrays.asList("/v3//ping", "/v3/./ping", "/v3/../ping")) {
      assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
          + "\"pathTemplate\":\"" + badSegmentTemplate + "\"}]",
          "Invalid segment");
    }
    for (String badCaptureTemplate : Arrays.asList("/v3/{}", "/v3/{a{b}}", "/v3/{a}b}")) {
      assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
          + "\"pathTemplate\":\"" + badCaptureTemplate + "\"}]",
          "Invalid capture");
    }
    for (String badBraceTemplate : Arrays.asList("/v3/pre{x}", "/v3/x}post")) {
      assertInvalidExtraRulesIgnored("[{\"service\":\"svc\",\"methods\":[\"GET\"],"
          + "\"pathTemplate\":\"" + badBraceTemplate + "\"}]",
          "Malformed brace in segment");
    }
  }

  private RuntimeCallbackAllowlist assertInvalidExtraRulesIgnored(String extraRulesJson,
      String expectedMessageSubstring) {
    CConfiguration badConf = CConfiguration.create();
    badConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES, extraRulesJson);
    Logger errorLogger = Mockito.mock(Logger.class);
    RuntimeCallbackAllowlist loaded = new RuntimeCallbackAllowlist(badConf, errorLogger);

    // Non-bundled path is denied while bundled rules remain active
    Assert.assertFalse(loaded.match("/v3/ping", "svc", "GET", tokenRunId, null).isAllowed());
    Assert.assertTrue(loaded.match("/v3/namespaces/runns/securekeys/k",
        Constants.Service.SECURE_STORE_SERVICE, "GET", tokenRunId, null).isAllowed());

    ArgumentCaptor<Throwable> thrownCaptor = ArgumentCaptor.forClass(Throwable.class);
    Mockito.verify(errorLogger, Mockito.times(1)).error(
        Mockito.anyString(),
        Mockito.eq(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES),
        thrownCaptor.capture());
    Mockito.verifyNoMoreInteractions(errorLogger);

    Throwable cause = thrownCaptor.getValue();
    Assert.assertNotNull("Expected exception message for: " + extraRulesJson, cause.getMessage());
    Assert.assertTrue("Expected message containing '" + expectedMessageSubstring
            + "', got: " + cause.getMessage(),
        cause.getMessage().contains(expectedMessageSubstring));
    return loaded;
  }

  @Test
  public void testScopeChecksOnAllowlist() {
    RuntimeScopes strictScopes = new RuntimeScopes(
        ImmutableSet.of("my_conn", "_"),
        ImmutableSet.of("github/cred1"),
        ImmutableSet.of("db-pass", "region-*-key"),
        false);

    // 1. Secret value & metadata: exact & pattern pass with null admission; others fail with SECRET
    RuntimeCallbackAllowlist.MatchResult exactSecret = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/db-pass", strictScopes);
    Assert.assertTrue(exactSecret.isAllowed());
    Assert.assertNull(exactSecret.getAdmission());

    RuntimeCallbackAllowlist.MatchResult patternSecretMeta = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/region-us-key/metadata", strictScopes);
    Assert.assertTrue(patternSecretMeta.isAllowed());
    Assert.assertNull(patternSecretMeta.getAdmission());

    RuntimeCallbackAllowlist.MatchResult deniedSecret = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/other-secret", strictScopes);
    Assert.assertFalse(deniedSecret.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.SECRET, deniedSecret.getFailure());
    Assert.assertEquals("/v3/namespaces/{namespace}/securekeys/{name}", deniedSecret.getLogPath());

    RuntimeCallbackAllowlist.MatchResult deniedSecretMeta = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/other-secret/metadata", strictScopes);
    Assert.assertFalse(deniedSecretMeta.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.SECRET, deniedSecretMeta.getFailure());
    Assert.assertEquals("/v3/namespaces/{namespace}/securekeys/{name}/metadata",
        deniedSecretMeta.getLogPath());

    // Wrong namespace fails with API_NOT_ALLOWED (before scope check)
    RuntimeCallbackAllowlist.MatchResult wrongNsSecret = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/otherns/securekeys/other-secret", strictScopes);
    Assert.assertFalse(wrongNsSecret.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.API_NOT_ALLOWED, wrongNsSecret.getFailure());

    // 2. Dynamic run: any secret in run's namespace admitted with DYNAMIC_SECRET; otherns denied
    RuntimeScopes dynamicScopes = new RuntimeScopes(
        Collections.emptySet(), Collections.emptySet(), Collections.singleton("db-pass"), true);
    RuntimeCallbackAllowlist.MatchResult dynamicExplicit = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/db-pass", dynamicScopes);
    Assert.assertTrue(dynamicExplicit.isAllowed());
    Assert.assertNull(dynamicExplicit.getAdmission());

    RuntimeCallbackAllowlist.MatchResult dynamicOther = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/runtime-secret", dynamicScopes);
    Assert.assertTrue(dynamicOther.isAllowed());
    Assert.assertEquals(RuntimeScopeAdmission.DYNAMIC_SECRET, dynamicOther.getAdmission());

    RuntimeCallbackAllowlist.MatchResult dynamicWrongNs = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/otherns/securekeys/runtime-secret", dynamicScopes);
    Assert.assertFalse(dynamicWrongNs.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.API_NOT_ALLOWED, dynamicWrongNs.getFailure());

    // 3. Connections: normalized IDs, literal '*', invalid segment, and wildcard '*'
    RuntimeCallbackAllowlist.MatchResult allowedConn = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/my+conn", strictScopes);
    Assert.assertTrue(allowedConn.isAllowed());
    Assert.assertNull(allowedConn.getAdmission());

    RuntimeCallbackAllowlist.MatchResult literalStarConn = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/*", strictScopes);
    Assert.assertTrue(literalStarConn.isAllowed());
    Assert.assertNull(literalStarConn.getAdmission());

    RuntimeCallbackAllowlist.MatchResult deniedConn = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/other-conn", strictScopes);
    Assert.assertFalse(deniedConn.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.CONNECTION, deniedConn.getFailure());
    Assert.assertEquals(
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/{namespace}"
            + "/connections/{connection}", deniedConn.getLogPath());

    RuntimeScopes wildcardConnScopes = new RuntimeScopes(
        Collections.singleton("*"), Collections.emptySet(), Collections.emptySet(), false);
    RuntimeCallbackAllowlist.MatchResult wildcardConn = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/any%20conn", wildcardConnScopes);
    Assert.assertTrue(wildcardConn.isAllowed());
    Assert.assertEquals(RuntimeScopeAdmission.WILDCARD_CONNECTION, wildcardConn.getAdmission());

    // Wrong namespace on connection fails with API_NOT_ALLOWED even with wildcard '*' connection
    RuntimeCallbackAllowlist.MatchResult wrongNsConn = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/otherns"
            + "/connections/my+conn", wildcardConnScopes);
    Assert.assertFalse(wrongNsConn.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.API_NOT_ALLOWED, wrongNsConn.getFailure());

    RuntimeCallbackAllowlist.MatchResult invalidConnSegment = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/contexts/runns"
            + "/connections/$bad", wildcardConnScopes);
    Assert.assertFalse(invalidConnSegment.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.CONNECTION, invalidConnSegment.getFailure());

    // 4. OAuth: exact, denied, wrong method (allowlist precedence), and wildcard per part
    RuntimeCallbackAllowlist.MatchResult exactOauth = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/github"
            + "/credential/cred1", strictScopes);
    Assert.assertTrue(exactOauth.isAllowed());
    Assert.assertNull(exactOauth.getAdmission());

    RuntimeCallbackAllowlist.MatchResult deniedOauth = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/github"
            + "/credential/cred2", strictScopes);
    Assert.assertFalse(deniedOauth.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.OAUTH, deniedOauth.getFailure());
    Assert.assertEquals(
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/{provider}"
            + "/credential/{credential}", deniedOauth.getLogPath());

    RuntimeCallbackAllowlist.MatchResult wrongMethodOauth = match(
        STUDIO_SERVICE, "DELETE",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/github"
            + "/credential/cred1", strictScopes);
    Assert.assertFalse(wrongMethodOauth.isAllowed());
    Assert.assertEquals(RuntimeValidationFailure.API_NOT_ALLOWED, wrongMethodOauth.getFailure());

    RuntimeScopes wildcardOauthScopes = new RuntimeScopes(
        Collections.emptySet(), Collections.singleton("github/*"), Collections.emptySet(), false);
    RuntimeCallbackAllowlist.MatchResult wildcardOauth = match(
        STUDIO_SERVICE, "GET",
        "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1/oauth/provider/github"
            + "/credential/cred2", wildcardOauthScopes);
    Assert.assertTrue(wildcardOauth.isAllowed());
    Assert.assertEquals(RuntimeScopeAdmission.WILDCARD_OAUTH, wildcardOauth.getAdmission());

    // 5. Null scopes (report-only run without scopes): skips scope check, still enforces allowlist
    RuntimeCallbackAllowlist.MatchResult nullScopesSecret = match(
        Constants.Service.SECURE_STORE_SERVICE, "GET",
        "/v3/namespaces/runns/securekeys/any-secret", null);
    Assert.assertTrue(nullScopesSecret.isAllowed());
    Assert.assertNull(nullScopesSecret.getAdmission());

    // 6. Extra rule with explicit scopeKind=NONE overrides both denied and dynamic-admitted
    //    bundled SECRET checks for a specific secret
    CConfiguration extraConf = CConfiguration.create();
    extraConf.set(Constants.RuntimeMonitor.RUNTIME_TOKEN_ALLOWLIST_EXTRA_RULES,
        "[{\"service\":\"secure.store.service\",\"methods\":[\"GET\"],"
            + "\"pathTemplate\":\"/v3/namespaces/{namespace}/securekeys/shared-key\","
            + "\"namespaceRule\":\"RUN\",\"scopeKind\":\"NONE\"}]");
    RuntimeCallbackAllowlist withExtra = new RuntimeCallbackAllowlist(extraConf);
    RuntimeCallbackAllowlist.MatchResult extraAllowed = withExtra.match(
        "/v3/namespaces/runns/securekeys/shared-key",
        Constants.Service.SECURE_STORE_SERVICE, "GET", tokenRunId, strictScopes);
    Assert.assertTrue(extraAllowed.isAllowed());
    Assert.assertNull(extraAllowed.getAdmission());

    RuntimeCallbackAllowlist.MatchResult extraOverridesDynamic = withExtra.match(
        "/v3/namespaces/runns/securekeys/shared-key",
        Constants.Service.SECURE_STORE_SERVICE, "GET", tokenRunId, dynamicScopes);
    Assert.assertTrue(extraOverridesDynamic.isAllowed());
    Assert.assertNull(extraOverridesDynamic.getAdmission());

    RuntimeCallbackAllowlist.MatchResult otherStillDynamic = withExtra.match(
        "/v3/namespaces/runns/securekeys/other-key",
        Constants.Service.SECURE_STORE_SERVICE, "GET", tokenRunId, dynamicScopes);
    Assert.assertTrue(otherStillDynamic.isAllowed());
    Assert.assertEquals(RuntimeScopeAdmission.DYNAMIC_SECRET, otherStillDynamic.getAdmission());
  }

  @Test
  public void testDatasetPutBodyRules() {
    // 1. externalDataset passes with any name and with properties ending in base.path
    RuntimeCallbackAllowlist.MatchResult foreignMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/any-external-name");
    Assert.assertTrue(foreignMatch.isAllowed());
    Assert.assertTrue(foreignMatch.requiresBodyCheck());

    Map<String, String> extProps = new HashMap<>();
    extProps.put("base.path", "/gs/bucket/data");
    extProps.put("files.base.path", "/gs/bucket/other");
    extProps.put("data.external", "true");
    Assert.assertTrue(allowlist.checkBody(foreignMatch, tokenRunId,
        datasetPutJson(ExternalDataset.TYPE, extProps)));

    // 2. Workflow-local dataset (*.<token run id>) passes for any dataset type with safe props
    RuntimeCallbackAllowlist.MatchResult localMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/conn-1." + tokenRunId.getRun());
    Assert.assertTrue(localMatch.requiresBodyCheck());
    Assert.assertTrue(allowlist.checkBody(localMatch, tokenRunId,
        datasetPutJson("table", Collections.singletonMap("schema", "{}"))));
    Assert.assertTrue(allowlist.checkBody(localMatch, tokenRunId,
        datasetPutJson(FileSet.TYPE, Collections.emptyMap())));
    // Omitted or null properties field passes
    Assert.assertTrue(allowlist.checkBody(localMatch, tokenRunId,
        "{\"typeName\":\"fileSet\"}".getBytes(StandardCharsets.UTF_8)));
    Assert.assertTrue(allowlist.checkBody(localMatch, tokenRunId,
        "{\"typeName\":\"fileSet\",\"properties\":null}".getBytes(StandardCharsets.UTF_8)));

    // 3. defaultCheckpointDataset passes with both FileSet type names
    RuntimeCallbackAllowlist.MatchResult checkpointMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/" + RuntimeCallbackAllowlist.DEFAULT_CHECKPOINT_DATASET);
    Assert.assertTrue(checkpointMatch.requiresBodyCheck());
    Assert.assertTrue(allowlist.checkBody(checkpointMatch, tokenRunId,
        datasetPutJson(FileSet.class.getName(), Collections.emptyMap())));
    Assert.assertTrue(allowlist.checkBody(checkpointMatch, tokenRunId,
        datasetPutJson(FileSet.TYPE, Collections.singletonMap("output.format", "text"))));
    // Non-FileSet type on defaultCheckpointDataset is denied
    Assert.assertFalse(allowlist.checkBody(checkpointMatch, tokenRunId,
        datasetPutJson("table", Collections.emptyMap())));

    // 4. Forbidden FileSet property suffixes fail on both workflow-local and checkpoint cases
    List<String> forbiddenSuffixes = Arrays.asList(
        FileSetProperties.BASE_PATH,
        FileSetProperties.DATA_EXTERNAL,
        FileSetProperties.DATA_USE_EXISTING,
        FileSetProperties.DATA_POSSESS_EXISTING);
    for (String suffix : forbiddenSuffixes) {
      for (String key : Arrays.asList(suffix, "files." + suffix)) {
        byte[] localBody = datasetPutJson(FileSet.TYPE, Collections.singletonMap(key, "true"));
        Assert.assertFalse("Expected workflow-local PUT denied for key " + key,
            allowlist.checkBody(localMatch, tokenRunId, localBody));
        byte[] checkpointBody = datasetPutJson(FileSet.class.getName(),
            Collections.singletonMap(key, "true"));
        Assert.assertFalse("Expected checkpoint PUT denied for key " + key,
            allowlist.checkBody(checkpointMatch, tokenRunId, checkpointBody));
      }
    }

    // 5. Foreign dataset name with non-externalDataset type fails
    Assert.assertFalse(allowlist.checkBody(foreignMatch, tokenRunId,
        datasetPutJson(FileSet.TYPE, Collections.emptyMap())));
    Assert.assertFalse(allowlist.checkBody(foreignMatch, tokenRunId,
        datasetPutJson("table", Collections.emptyMap())));

    // 6. Local dataset for another run id fails
    RuntimeCallbackAllowlist.MatchResult otherRunLocal = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/conn-1." + RunIds.generate().getId());
    Assert.assertFalse(allowlist.checkBody(otherRunLocal, tokenRunId,
        datasetPutJson(FileSet.TYPE, Collections.emptyMap())));

    // 7. Malformed, empty, null, or missing typeName body fails
    Assert.assertFalse(allowlist.checkBody(localMatch, tokenRunId, new byte[0]));
    Assert.assertFalse(allowlist.checkBody(localMatch, tokenRunId,
        "not-json".getBytes(StandardCharsets.UTF_8)));
    Assert.assertFalse(allowlist.checkBody(localMatch, tokenRunId,
        "null".getBytes(StandardCharsets.UTF_8)));
    Assert.assertFalse(allowlist.checkBody(localMatch, tokenRunId,
        "{}".getBytes(StandardCharsets.UTF_8)));
    Assert.assertFalse(allowlist.checkBody(localMatch, tokenRunId,
        "{\"typeName\":\"\"}".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testDatasetPutOwnerPrincipalRespectsKerberosEnabled() {
    RuntimeCallbackAllowlist.MatchResult localMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/conn-1." + tokenRunId.getRun());
    RuntimeCallbackAllowlist.MatchResult checkpointMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/"
            + RuntimeCallbackAllowlist.DEFAULT_CHECKPOINT_DATASET);
    RuntimeCallbackAllowlist.MatchResult externalMatch = match(
        Constants.Service.DATASET_MANAGER, "PUT",
        "/v3/namespaces/runns/data/datasets/any-external-name");

    String ownerPrincipal = "alice/host@EXAMPLE.COM";
    byte[] fileSetWithPrincipal = datasetPutJson(
        FileSet.TYPE, Collections.emptyMap(), ownerPrincipal);
    byte[] externalWithPrincipal = datasetPutJson(
        ExternalDataset.TYPE, Collections.emptyMap(), ownerPrincipal);

    byte[] fileSetWithoutPrincipal = datasetPutJson(FileSet.TYPE, Collections.emptyMap());
    byte[] externalWithoutPrincipal = datasetPutJson(ExternalDataset.TYPE, Collections.emptyMap());
    byte[] externalEmptyPrincipal = datasetPutJson(ExternalDataset.TYPE, Collections.emptyMap(), "");

    byte[] fileSetNullPrincipal =
        "{\"typeName\":\"fileSet\",\"properties\":{},\"principal\":null}"
            .getBytes(StandardCharsets.UTF_8);
    byte[] externalNullPrincipal =
        "{\"typeName\":\"externalDataset\",\"properties\":{},\"principal\":null}"
            .getBytes(StandardCharsets.UTF_8);

    // Kerberos disabled: both default CConfiguration and security.enabled=true with
    // kerberos.auth.enabled=false reject any non-null principal across all three dataset kinds,
    // while accepting bodies without principal or with explicit "principal": null.
    CConfiguration nonKerberosSecConf = CConfiguration.create();
    nonKerberosSecConf.setBoolean(Constants.Security.ENABLED, true);
    nonKerberosSecConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    Map<String, RuntimeCallbackAllowlist> nonKerberosAllowlists = new LinkedHashMap<>();
    nonKerberosAllowlists.put("default CConfiguration", allowlist);
    nonKerberosAllowlists.put("security.enabled=true, kerberos.auth.enabled=false",
        new RuntimeCallbackAllowlist(nonKerberosSecConf));
    for (Map.Entry<String, RuntimeCallbackAllowlist> entry : nonKerberosAllowlists.entrySet()) {
      String confName = entry.getKey();
      RuntimeCallbackAllowlist nonKerberosAllowlist = entry.getValue();

      Assert.assertFalse(confName, nonKerberosAllowlist.checkBody(
          localMatch, tokenRunId, fileSetWithPrincipal));
      Assert.assertFalse(confName, nonKerberosAllowlist.checkBody(
          checkpointMatch, tokenRunId, fileSetWithPrincipal));
      Assert.assertFalse(confName, nonKerberosAllowlist.checkBody(
          externalMatch, tokenRunId, externalWithPrincipal));

      // Empty-string principal is also non-null and must be rejected.
      Assert.assertFalse(confName, nonKerberosAllowlist.checkBody(
          externalMatch, tokenRunId, externalEmptyPrincipal));

      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          localMatch, tokenRunId, fileSetWithoutPrincipal));
      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          checkpointMatch, tokenRunId, fileSetWithoutPrincipal));
      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          externalMatch, tokenRunId, externalWithoutPrincipal));

      // Explicit JSON "principal": null deserializes to null and is accepted.
      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          localMatch, tokenRunId, fileSetNullPrincipal));
      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          checkpointMatch, tokenRunId, fileSetNullPrincipal));
      Assert.assertTrue(confName, nonKerberosAllowlist.checkBody(
          externalMatch, tokenRunId, externalNullPrincipal));
    }

    // Kerberos enabled: bodies with "principal" are accepted as before.
    CConfiguration kerberosConf = CConfiguration.create();
    kerberosConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
    RuntimeCallbackAllowlist kerberosAllowlist = new RuntimeCallbackAllowlist(kerberosConf);
    Assert.assertTrue(kerberosAllowlist.checkBody(localMatch, tokenRunId, fileSetWithPrincipal));
    Assert.assertTrue(kerberosAllowlist.checkBody(
        checkpointMatch, tokenRunId, fileSetWithPrincipal));
    Assert.assertTrue(kerberosAllowlist.checkBody(
        externalMatch, tokenRunId, externalWithPrincipal));
  }

  private static byte[] datasetPutJson(String typeName, Map<String, String> properties) {
    return datasetPutJson(typeName, properties, null);
  }

  private static byte[] datasetPutJson(String typeName, Map<String, String> properties,
      @Nullable String ownerPrincipal) {
    return GSON.toJson(new DatasetInstanceConfiguration(typeName, properties, null, ownerPrincipal))
        .getBytes(StandardCharsets.UTF_8);
  }

  private static Map<String, String> mapOf(String... kv) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      map.put(kv[i], kv[i + 1]);
    }
    return map;
  }

  private static final class ParityCase {
    private final String service;
    private final String method;
    private final String servicePath;
    private final String expectedTemplate;
    private final Map<String, String> expectedCaptures;

    private ParityCase(String service, String method, String servicePath,
        String expectedTemplate, Map<String, String> expectedCaptures) {
      this.service = service;
      this.method = method;
      this.servicePath = servicePath;
      this.expectedTemplate = expectedTemplate;
      this.expectedCaptures = expectedCaptures;
    }
  }

  private static final class ParityResponse {
    private String uri;
    private String template;
    private Map<String, String> captures;
  }

  /**
   * Public handler registering the same path templates as {@code runtime-callback-allowlist.json}
   * to verify exact routing and capture parity with {@link NettyHttpService}.
   */
  public static final class ParityHandler extends AbstractHttpHandler {

    private static void send(HttpRequest req, HttpResponder responder, String template,
        String... kv) {
      ParityResponse resp = new ParityResponse();
      resp.uri = req.uri();
      resp.template = template;
      resp.captures = mapOf(kv);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(resp));
    }

    @GET
    @Path("/v3/namespaces/{namespace}/securekeys/{name}")
    public void getSecret(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      send(req, responder, "/v3/namespaces/{namespace}/securekeys/{name}",
          "namespace", namespace, "name", name);
    }

    @GET
    @Path("/v3/namespaces/{namespace}/securekeys/{name}/metadata")
    public void getSecretMetadata(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      send(req, responder, "/v3/namespaces/{namespace}/securekeys/{name}/metadata",
          "namespace", namespace, "name", name);
    }

    @GET
    @Path("/v3Internal/namespaces/{namespace}/artifacts/{artifact}/versions/{version}/download")
    public void downloadArtifact(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("artifact") String artifact,
        @PathParam("version") String version) {
      send(req, responder,
          "/v3Internal/namespaces/{namespace}/artifacts/{artifact}/versions/{version}/download",
          "namespace", namespace, "artifact", artifact, "version", version);
    }

    @GET
    @Path("/v3/namespaces/{namespace}/artifacts/{artifact}/versions/{version}"
        + "/extensions/{type}/plugins/{plugin}")
    public void getPlugin(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("artifact") String artifact,
        @PathParam("version") String version, @PathParam("type") String type,
        @PathParam("plugin") String plugin) {
      send(req, responder,
          "/v3/namespaces/{namespace}/artifacts/{artifact}/versions/{version}"
              + "/extensions/{type}/plugins/{plugin}",
          "namespace", namespace, "artifact", artifact, "version", version,
          "type", type, "plugin", plugin);
    }

    @GET
    @PUT
    @DELETE
    @Path("/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}")
    public void appState(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("app") String app,
        @PathParam("key") String key) {
      send(req, responder, "/v3Internal/namespaces/{namespace}/apps/{app}/states/{key}",
          "namespace", namespace, "app", app, "key", key);
    }

    @GET
    @Path("/v3/namespaces/{namespace}")
    public void getNamespace(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace) {
      send(req, responder, "/v3/namespaces/{namespace}", "namespace", namespace);
    }

    @GET
    @Path("/v3/namespaces/system/apps/pipeline/services/studio/methods/v1"
        + "/contexts/{namespace}/connections/{connection}")
    public void getConnection(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("connection") String connection) {
      send(req, responder,
          "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1"
              + "/contexts/{namespace}/connections/{connection}",
          "namespace", namespace, "connection", connection);
    }

    @GET
    @Path("/v3/namespaces/system/apps/pipeline/services/studio/methods/v1"
        + "/oauth/provider/{provider}/credential/{credential}")
    public void getOauth(HttpRequest req, HttpResponder responder,
        @PathParam("provider") String provider, @PathParam("credential") String credential) {
      send(req, responder,
          "/v3/namespaces/system/apps/pipeline/services/studio/methods/v1"
              + "/oauth/provider/{provider}/credential/{credential}",
          "provider", provider, "credential", credential);
    }

    @GET
    @Path(DATAPREP_CONFIG_PATH)
    public void getDataprepConfig(HttpRequest req, HttpResponder responder) {
      send(req, responder, DATAPREP_CONFIG_PATH);
    }

    @GET
    @PUT
    @DELETE
    @Path("/v3/namespaces/{namespace}/data/datasets/{name}")
    public void dataset(HttpRequest req, HttpResponder responder,
        @PathParam("namespace") String namespace, @PathParam("name") String name) {
      send(req, responder, "/v3/namespaces/{namespace}/data/datasets/{name}",
          "namespace", namespace, "name", name);
    }
  }
}
