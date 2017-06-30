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

package co.cask.cdap.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.http.CommonNettyHttpServiceBuilder;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.gateway.handlers.SecureStoreHandler;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.security.SecureKeyCreateRequest;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
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
import java.util.Map;

public class SecureStoreTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
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
      new SecureStoreModules().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(AuthorizationEnforcer.class).to(NoOpAuthorizer.class);
          bind(NamespaceAdmin.class).to(InMemoryNamespaceClient.class).in(Scopes.SINGLETON);
          bind(NamespaceQueryAdmin.class).to(NamespaceAdmin.class);
        }
      }
    );

    injector.getInstance(NamespaceAdmin.class).create(NamespaceMeta.DEFAULT);

    httpServer = new CommonNettyHttpServiceBuilder(injector.getInstance(CConfiguration.class), "SecureStore")
      .addHttpHandlers(Collections.singleton(injector.getInstance(SecureStoreHandler.class)))
      .build();
    httpServer.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    httpServer.stopAndWait();
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
    Map<String, String> keys = GSON.fromJson(response.getResponseBodyAsString(), MAP_TYPE);
    Assert.assertTrue(keys.isEmpty());

    // One element
    SecureKeyCreateRequest secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION, DATA, PROPERTIES);
    response = create(KEY, secureKeyCreateRequest);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), MAP_TYPE);
    Assert.assertEquals(1, keys.size());
    Assert.assertEquals(DESCRIPTION, keys.get(KEY));

    // Two elements
    secureKeyCreateRequest = new SecureKeyCreateRequest(DESCRIPTION2, DATA2, PROPERTIES2);
    response = create(KEY2, secureKeyCreateRequest);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), MAP_TYPE);
    Assert.assertEquals(2, keys.size());
    Assert.assertEquals(DESCRIPTION, keys.get(KEY));
    Assert.assertEquals(DESCRIPTION2, keys.get(KEY2));


    // After deleting an element
    response = delete(KEY);
    Assert.assertEquals(200, response.getResponseCode());
    response = list();
    Assert.assertEquals(200, response.getResponseCode());
    keys = GSON.fromJson(response.getResponseBodyAsString(), MAP_TYPE);
    Assert.assertEquals(1, keys.size());
    Assert.assertEquals(DESCRIPTION2, keys.get(KEY2));
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
