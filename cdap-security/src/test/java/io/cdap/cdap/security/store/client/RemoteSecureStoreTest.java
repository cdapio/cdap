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

package io.cdap.cdap.security.store.client;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.SecureKeyNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.security.store.FileSecureStoreService;
import io.cdap.cdap.security.store.SecureStoreHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests for {@link RemoteSecureStore}.
 */
public class RemoteSecureStoreTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final String NAMESPACE1 = "ns1";
  private static NettyHttpService httpService;
  private static RemoteSecureStore remoteSecureStore;

  @BeforeClass
  public static void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, true);
    conf.set(Constants.Security.Store.FILE_PATH, TEMP_FOLDER.newFolder().getAbsolutePath());
    SConfiguration sConf = SConfiguration.create();
    sConf.set(Constants.Security.Store.FILE_PASSWORD, "secret");
    InMemoryNamespaceAdmin namespaceClient = new InMemoryNamespaceAdmin();

    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder()
      .setName(NAMESPACE1)
      .build();
    namespaceClient.create(namespaceMeta);

    FileSecureStoreService fileSecureStoreService = new FileSecureStoreService(conf, sConf, namespaceClient);
    // Starts a mock server to handle remote secure store requests
    httpService = new HttpsEnabler().configureKeyStore(conf, sConf).enable(
      NettyHttpService.builder("remoteSecureStoreTest")
      .setHttpHandlers(new SecureStoreHandler(fileSecureStoreService, fileSecureStoreService))
      .setExceptionHandler(new HttpExceptionHandler()))
      .build();

    httpService.start();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    discoveryService.register(URIScheme.HTTPS.createDiscoverable(Constants.Service.SECURE_STORE_SERVICE,
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
    remoteSecureStore.put(NAMESPACE1, "key", "value", "description", ImmutableMap.of("prop1", "value1"));
    SecureStoreData actual = remoteSecureStore.get(NAMESPACE1, "key");
    Assert.assertEquals(secureStoreMetadata.getName(), actual.getMetadata().getName());
    Assert.assertArrayEquals(secureStoreData.get(), actual.get());
    Assert.assertEquals(secureStoreMetadata.getDescription(), actual.getMetadata().getDescription());
    Assert.assertEquals(secureStoreMetadata.getProperties().size(), actual.getMetadata().getProperties().size());

    // test list
    List<SecureStoreMetadata> secureData = remoteSecureStore.list(NAMESPACE1);
    Assert.assertEquals(1, secureData.size());
    SecureStoreMetadata metadata = secureData.get(0);
    Assert.assertEquals("key", metadata.getName());
    Assert.assertEquals("description", metadata.getDescription());

    // test delete
    remoteSecureStore.delete(NAMESPACE1, "key");
    Assert.assertEquals(0, remoteSecureStore.list(NAMESPACE1).size());
  }

  @Test(expected = SecureKeyNotFoundException.class)
  public void testKeyNotFound() throws Exception {
    remoteSecureStore.get(NAMESPACE1, "nonexistingkey");
  }
}
