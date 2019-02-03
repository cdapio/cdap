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

package co.cask.cdap.security;

import co.cask.cdap.data.security.DefaultSecretStore;
import co.cask.cdap.securestore.spi.SecretNotFoundException;
import co.cask.cdap.securestore.spi.SecretStore;
import co.cask.cdap.securestore.spi.secret.Decoder;
import co.cask.cdap.securestore.spi.secret.Encoder;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Tests for {@link DefaultSecretStore}.
 */
public abstract class DefaultSecretStoreTest {
  protected static SecretStore store;

  @Test
  public void testSecretStore() throws Exception {
    String namespace = "ns1";
    String name = "secretKey";
    String description = "description";
    String data = "password";
    long now = System.currentTimeMillis();
    Map<String, String> map = ImmutableMap.of("p1", "v1");
    FakeEncoder fakeEncoder = new FakeEncoder();
    FakeDecoder fakeDecoder = new FakeDecoder();
    TestSecret expected = new TestSecret(name, description,
                                         data.getBytes(StandardCharsets.UTF_8), now, map);

    store.store(namespace, name, fakeEncoder, expected);
    TestSecret actual = store.get(namespace, name, fakeDecoder);
    Assert.assertEquals(expected, actual);

    store.delete(namespace, name);
    try {
      store.get(name, name, fakeDecoder);
      Assert.fail("Expected SecretNotFoundException");
    } catch (SecretNotFoundException e) {
      // expected
    }

    try {
      store.delete(namespace, name);
      Assert.fail("Expected SecretNotFoundException");
    } catch (SecretNotFoundException e) {
      // expected
    }

    List<TestSecret> expectedList = new ArrayList<>();
    expected = new TestSecret(name, description,
                              data.getBytes(StandardCharsets.UTF_8), now, map);
    expectedList.add(expected);
    store.store(namespace, name, fakeEncoder, expected);
    expected = new TestSecret("secretKey2", description,
                              data.getBytes(StandardCharsets.UTF_8), now, map);
    expectedList.add(expected);
    store.store(namespace, "secretKey2", fakeEncoder, expected);

    store.store("ns2", name, fakeEncoder, expected);

    List<TestSecret> actualList = new ArrayList<>(store.list(namespace, fakeDecoder));

    Assert.assertEquals(expectedList, actualList);
  }

  private static class TestSecret {
    private final String name;
    private final String description;
    private final byte[] secretData;
    private final long creationTimeMs;
    private final Map<String, String> properties;

    TestSecret(String name, String description, byte[] secretData, long creationTimeMs,
               Map<String, String> properties) {
      this.name = name;
      this.description = description;
      this.secretData = secretData;
      this.creationTimeMs = creationTimeMs;
      this.properties = properties;
    }

    String getName() {
      return name;
    }

    String getDescription() {
      return description;
    }

    byte[] getSecretData() {
      return secretData;
    }

    long getCreationTimeMs() {
      return creationTimeMs;
    }

    Map<String, String> getProperties() {
      return properties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestSecret secret = (TestSecret) o;

      return creationTimeMs == secret.creationTimeMs &&
        Objects.equals(name, secret.name) &&
        Objects.equals(description, secret.description) &&
        Arrays.equals(secretData, secret.secretData) &&
        Objects.equals(properties, secret.properties);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(name, description, creationTimeMs, properties);
      result = 31 * result + Arrays.hashCode(secretData);
      return result;
    }
  }

  private static class FakeEncoder implements Encoder<TestSecret> {
    @Override
    public byte[] encode(TestSecret data) throws IOException {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        dos.writeUTF(data.getName());
        dos.writeBoolean(data.getDescription() != null);
        if (data.getDescription() != null) {
          dos.writeUTF(data.getDescription());
        }
        dos.writeLong(data.getCreationTimeMs());

        Map<String, String> properties = data.getProperties();
        dos.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
          dos.writeUTF(entry.getKey());
          dos.writeUTF(entry.getValue());
        }

        byte[] secret = data.getSecretData();
        dos.writeInt(secret.length);
        dos.write(secret);
      }
      return bos.toByteArray();
    }
  }

  private static class FakeDecoder implements Decoder<TestSecret> {
    @Override
    public TestSecret decode(byte[] data) throws IOException {
      try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
        String name = dis.readUTF();
        boolean descriptionExists = dis.readBoolean();
        String description = descriptionExists ? dis.readUTF() : null;
        long creationTimeMs = dis.readLong();

        Map<String, String> properties = new HashMap<>();
        int len = dis.readInt();
        for (int i = 0; i < len; i++) {
          properties.put(dis.readUTF(), dis.readUTF());
        }

        byte[] secret = new byte[dis.readInt()];
        dis.readFully(secret);
        return new TestSecret(name, description, secret, creationTimeMs, properties);
      }
    }
  }
}
