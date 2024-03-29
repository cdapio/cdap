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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.SecureKeyNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.SecureKeyCreateRequest;
import io.cdap.cdap.test.SingletonExternalResource;
import io.cdap.cdap.test.XSlowTests;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link SecureStoreClient}
 */
@Category(XSlowTests.class)
public class SecureStoreClientTest extends AbstractClientTest {

  private SecureStoreClient client;

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(
    new StandaloneTester(Constants.Security.Store.PROVIDER, "file"));

  @Override
  protected StandaloneTester getStandaloneTester() {
    return STANDALONE.get();
  }

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    client = new SecureStoreClient(clientConfig);
  }

  @Test
  public void testErrorScenarios() throws Exception {
    try {
      client.listKeys(new NamespaceId("notfound"));
      Assert.fail("Should have thrown exception since namespace doesn't exist");
    } catch (NamespaceNotFoundException e) {
      // expected
    }

    try {
      client.deleteKey(new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "badkey"));
      Assert.fail("Should have thrown exception since the key doesn't exist");
    } catch (SecureKeyNotFoundException e) {
      // expected
    }

    try {
      client.getData(new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "badkey"));
      Assert.fail("Should have thrown exception since the key doesn't exist");
    } catch (SecureKeyNotFoundException e) {
      // expected
    }

    try {
      client.getKeyMetadata(new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "badkey"));
      Assert.fail("Should have thrown exception since the key doesn't exist");
    } catch (SecureKeyNotFoundException e) {
      // expected
    }

    try {
      client.getKeyMetadata(new SecureKeyId("notfound", "somekey"));
      Assert.fail("Should have thrown exception since the namespace doesn't exist");
    } catch (SecureKeyNotFoundException e) {
      // expected
    }

    SecureKeyId id = new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), "key1");
    SecureKeyCreateRequest request = new SecureKeyCreateRequest("", "a", ImmutableMap.<String, String>of());
    client.createKey(id, request);
    client.deleteKey(id);
  }

  @Test
  public void testSecureKeys() throws Exception {
    // no secure keys to begin with
    List<SecureStoreMetadata> secureKeys = client.listKeys(NamespaceId.DEFAULT);
    Assert.assertTrue(secureKeys.isEmpty());

    // create a key
    String key = "securekey";
    String desc = "SomeDesc";
    String data = "secureData";
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    long creationTime = System.currentTimeMillis();
    SecureKeyId secureKeyId = new SecureKeyId(NamespaceId.DEFAULT.getNamespace(), key);
    client.createKey(secureKeyId, new SecureKeyCreateRequest(desc, data, properties));
    Assert.assertEquals(data, client.getData(secureKeyId));
    Assert.assertEquals(1, client.listKeys(NamespaceId.DEFAULT).size());
    SecureStoreMetadata metadata = client.getKeyMetadata(secureKeyId);
    Assert.assertEquals(desc, metadata.getDescription());
    Assert.assertTrue(metadata.getLastModifiedTime() >= creationTime);
    Assert.assertEquals(properties, metadata.getProperties());

    client.createKey(secureKeyId, new SecureKeyCreateRequest(desc, "updatedSecureData", properties));
    Assert.assertEquals("updatedSecureData", client.getData(secureKeyId));
    Assert.assertEquals(1, client.listKeys(NamespaceId.DEFAULT).size());
    metadata = client.getKeyMetadata(secureKeyId);
    Assert.assertEquals(desc, metadata.getDescription());
    Assert.assertTrue(metadata.getLastModifiedTime() >= creationTime);
    Assert.assertEquals(properties, metadata.getProperties());
    // delete the key
    client.deleteKey(secureKeyId);
    Assert.assertTrue(client.listKeys(NamespaceId.DEFAULT).isEmpty());
  }
}
