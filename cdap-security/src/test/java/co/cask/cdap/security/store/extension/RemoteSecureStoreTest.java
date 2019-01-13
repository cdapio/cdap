/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.security.store.extension;

import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.SecureKeyNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.SecureKeyId;
import co.cask.cdap.security.store.client.RemoteSecureStore;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Tests for {@link RemoteSecureStore}.
 */
public class RemoteSecureStoreTest {
  private static NettyHttpService httpService;
  private static RemoteSecureStore remoteSecureStore;

  @BeforeClass
  public static void setUp() throws Exception {
    // Starts a mock server to handle remote secure store requests
    httpService = NettyHttpService.builder("remoteSecureStoreTest")
      .setHttpHandlers(new SecureStoreTestHandler())
      .setExceptionHandler(new HttpExceptionHandler())
      .build();

    httpService.start();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    discoveryService.register(new Discoverable(Constants.Security.Store.SECURE_STORE_SERVICE,
                                               httpService.getBindAddress()));

    remoteSecureStore = new RemoteSecureStore(discoveryService);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    httpService.stop();
  }

  @Test
  public void testRemoteSecureStore() throws Exception {
    SecureStoreMetadata secureStoreMetadata = new SecureStoreMetadata("key", "description", 1,
                                                                      ImmutableMap.of("prop1", "value1"));
    SecureStoreData secureStoreData = new SecureStoreData(secureStoreMetadata,
                                                          "value".getBytes(StandardCharsets.UTF_8));

    // test put and get
    remoteSecureStore.putSecureData("ns1", "key", "value", "description", ImmutableMap.of("prop1", "value1"));
    SecureStoreData actual = remoteSecureStore.getSecureData("ns1", "key");
    Assert.assertEquals(secureStoreMetadata.getName(), actual.getMetadata().getName());
    Assert.assertArrayEquals(secureStoreData.get(), actual.get());
    Assert.assertEquals(secureStoreMetadata.getDescription(), actual.getMetadata().getDescription());
    Assert.assertEquals(secureStoreMetadata.getProperties().size(), actual.getMetadata().getProperties().size());

    // test list
    Map<String, String> secureData = remoteSecureStore.listSecureData("ns1");
    Assert.assertEquals(1, secureData.size());
    Map.Entry<String, String> entry = secureData.entrySet().iterator().next();
    Assert.assertEquals("key", entry.getKey());
    Assert.assertEquals("description", entry.getValue());

    // test delete
    remoteSecureStore.deleteSecureData("ns1", "key");
    Assert.assertEquals(0, remoteSecureStore.listSecureData("ns1").size());
  }

  @Test(expected = SecureKeyNotFoundException.class)
  public void testKeyNotFound() throws Exception {
    remoteSecureStore.getSecureData("ns1", "nonexistingkey");
  }

  /**
   * A http handler to provide the secure store end points for {@link RemoteSecureStore}.
   */
  @Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/securekeys")
  public static final class SecureStoreTestHandler extends AbstractHttpHandler {

    private static final Gson GSON = new Gson();
    private static Map<String, SecureStoreData> map = new HashMap<>();

    @Path("/{key-name}")
    @PUT
    public void create(FullHttpRequest httpRequest, HttpResponder httpResponder,
                       @PathParam("namespace-id") String namespace,
                       @PathParam("key-name") String name) throws Exception {
      map.put(getKey(namespace, name),
              new SecureStoreData(new SecureStoreMetadata(name, "description", 1, ImmutableMap.of("prop1", "value1")),
                                  "value".getBytes(StandardCharsets.UTF_8)));
      httpResponder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/{key-name}")
    @DELETE
    public void delete(HttpRequest httpRequest, HttpResponder httpResponder,
                       @PathParam("namespace-id") String namespace,
                       @PathParam("key-name") String name) throws Exception {
      String key = getKey(namespace, name);
      if (!map.containsKey(key)) {
        throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
      }
      map.remove(key);
      httpResponder.sendStatus(HttpResponseStatus.OK);
    }

    @Path("/{key-name}")
    @GET
    public void get(HttpRequest httpRequest, HttpResponder httpResponder, @PathParam("namespace-id") String namespace,
                    @PathParam("key-name") String name) throws Exception {
      String key = getKey(namespace, name);
      if (!map.containsKey(key)) {
        throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
      }
      SecureStoreData secureStoreData = map.get(key);
      httpResponder.sendByteArray(HttpResponseStatus.OK, secureStoreData.get(),
                                  new DefaultHttpHeaders().set(HttpHeaderNames.CONTENT_TYPE,
                                                               "text/plain;charset=utf-8"));
    }

    @Path("/{key-name}/metadata")
    @GET
    public void getMetadata(HttpRequest httpRequest, HttpResponder httpResponder,
                            @PathParam("namespace-id") String namespace,
                            @PathParam("key-name") String name) throws Exception {
      String key = getKey(namespace, name);
      if (!map.containsKey(key)) {
        throw new SecureKeyNotFoundException(new SecureKeyId(namespace, name));
      }
      SecureStoreData secureStoreData = map.get(key);
      httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(secureStoreData.getMetadata()));
    }

    @Path("/")
    @GET
    public void list(HttpRequest httpRequest, HttpResponder httpResponder,
                     @PathParam("namespace-id") String namespace) throws Exception {
      Map<String, String> list = new HashMap<>();
      for (Map.Entry<String, SecureStoreData> entry : map.entrySet()) {
        String[] splitted = entry.getKey().split(":");
        if (splitted[0].equals(namespace)) {
          list.put( entry.getValue().getMetadata().getName(), entry.getValue().getMetadata().getDescription());
        }
      }
      httpResponder.sendJson(HttpResponseStatus.OK, GSON.toJson(list));
    }

    private static String getKey(String namespace, String name) {
      return namespace + ":" + name;
    }
  }
}
