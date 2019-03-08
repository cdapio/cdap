/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.namespace.InMemoryNamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.guice.SecureStoreServerModule;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.http.NettyHttpService;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class SecureStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final Type LIST_TYPE = new TypeToken<List<SecureStoreMetadata>>() { }.getType();
  private static final String KEY = "key1";
  private static final String DESCRIPTION = "This is Key1";
  private static final String DATA = "Secret1";
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("Prop1", "Val1", "Prop2", "Val2");
  private static final String KEY2 = "key2";
  private static final String DESCRIPTION2 = "This is Key2";
  private static final String DATA2 = "Secret2";
  private static final Map<String, String> PROPERTIES2 = ImmutableMap.of("Prop1", "Val1", "Prop2", "Val2");

  private static NettyHttpService httpServer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Security.Store.PROVIDER, "file");

    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, new Configuration(), sConf),
      new SecureStoreServerModule(),
      new AuthorizationTestModule(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuthorizationEnforcer.class).to(NoOpAuthorizer.class);
          bind(NamespaceAdmin.class).to(InMemoryNamespaceAdmin.class).in(Scopes.SINGLETON);
          bind(NamespaceQueryAdmin.class).to(NamespaceAdmin.class);
        }
      }
    );

    injector.getInstance(NamespaceAdmin.class).create(NamespaceMeta.DEFAULT);

    httpServer = new CommonNettyHttpServiceBuilder(injector.getInstance(CConfiguration.class), "SecureStore")
      .setHttpHandlers(Collections.singleton(injector.getInstance(SecureStoreHandler.class)))
      .build();
    httpServer.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    httpServer.stop();
  }

  private URL getURL(String path) throws MalformedURLException {
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    InetSocketAddress addr = httpServer.getBindAddress();
    return new URL(String.format("http://%s:%d%s", addr.getHostName(), addr.getPort(), path));
  }


  @Test
  public void testCreate() throws Exception {
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA,
                                                                               PROPERTIES);
    HttpResponse response = create(KEY, secureKeyCreateRequest);
    Assert.assertEquals(200, response.getResponseCode());

    response = get(KEY);
    Assert.assertEquals(200, response.getResponseCode());
    Assert.assertEquals(DATA, response.getResponseBodyAsString());

    response = delete(KEY);
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testList() throws Exception {
    // Test empty list
    HttpResponse response = list();
    Assert.assertEquals(200, response.getResponseCode());
    List<SecureStoreMetadata> keys = GSON.fromJson(response.getResponseBodyAsString(), LIST_TYPE);
    Assert.assertTrue(keys.isEmpty());

    // One element
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA, PROPERTIES);
    response = create(KEY, secureKeyCreateRequest);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), LIST_TYPE);
    Assert.assertEquals(1, keys.size());
    Assert.assertEquals(DESCRIPTION, keys.get(0).getDescription());

    // Two elements
    secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION2, DATA2, PROPERTIES2);
    response = create(KEY2, secureKeyCreateRequest);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), LIST_TYPE);
    Assert.assertEquals(2, keys.size());
    keys.sort(Comparator.comparing(SecureStoreMetadata::getName));
    Assert.assertEquals(DESCRIPTION, keys.get(0).getDescription());
    Assert.assertEquals(DESCRIPTION2, keys.get(1).getDescription());


    // After deleting an element
    response = delete(KEY);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), LIST_TYPE);
    Assert.assertEquals(1, keys.size());
    Assert.assertEquals(DESCRIPTION2, keys.get(0).getDescription());
  }

  public HttpResponse create(String key, SecureKeyCreateRequest keyCreateRequest) throws Exception {
    return HttpRequests.execute(HttpRequest.put(getURL("/v3/namespaces/default/securekeys/" + key))
                                  .withBody(GSON.toJson(keyCreateRequest)).build());
  }

  public HttpResponse get(String key) throws Exception {
    return HttpRequests.execute(HttpRequest.get(getURL("/v3/namespaces/default/securekeys/" + key)).build());
  }

  public HttpResponse delete(String key) throws Exception {
    return HttpRequests.execute(HttpRequest.delete(getURL("/v3/namespaces/default/securekeys/" + key)).build());
  }

  public HttpResponse list() throws Exception {
    return HttpRequests.execute(HttpRequest.get(getURL("/v3/namespaces/default/securekeys")).build());
  }
}
