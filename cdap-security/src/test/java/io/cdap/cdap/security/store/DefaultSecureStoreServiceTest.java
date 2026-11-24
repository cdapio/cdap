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

package io.cdap.cdap.security.store;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.proto.id.SecureKeyId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultSecureStoreService}.
 */
public class DefaultSecureStoreServiceTest {

  @Test
  public void testStartAndStopDelegatesToBackingServiceAndEnforcesAccess() throws Exception {
    AccessEnforcer accessEnforcer = Mockito.mock(AccessEnforcer.class);
    AuthenticationContext authContext = new AuthenticationTestContext();
    Principal principal = authContext.getPrincipal();
    InMemoryBackingSecureStoreService backingService = new InMemoryBackingSecureStoreService();

    DefaultSecureStoreService service = new DefaultSecureStoreService(accessEnforcer, authContext, backingService);
    Assert.assertFalse(backingService.isRunning());

    service.startAsync().awaitRunning();
    Assert.assertTrue(service.isRunning());
    Assert.assertTrue(backingService.isRunning());

    try {
      service.put("ns1", "k1", "secret-val", "desc", Collections.singletonMap("env", "prod"));
      Mockito.verify(accessEnforcer).enforce(new SecureKeyId("ns1", "k1"), principal, StandardPermission.UPDATE);

      SecureStoreData stored = service.get("ns1", "k1");
      Mockito.verify(accessEnforcer).enforce(new SecureKeyId("ns1", "k1"), principal, StandardPermission.GET);
      Assert.assertEquals("secret-val", new String(stored.get(), StandardCharsets.UTF_8));

      Assert.assertEquals(1, service.list("ns1").size());
      service.delete("ns1", "k1");
      Mockito.verify(accessEnforcer).enforce(new SecureKeyId("ns1", "k1"), principal, StandardPermission.DELETE);
      Assert.assertTrue(service.list("ns1").isEmpty());
    } finally {
      service.stopAsync().awaitTerminated();
    }

    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(Service.State.TERMINATED, backingService.state());
  }

  private static final class InMemoryBackingSecureStoreService extends DummySecureStoreService {
    private final Map<String, SecureStoreData> store = new HashMap<>();

    @Override
    public List<SecureStoreMetadata> list(String namespace) {
      List<SecureStoreMetadata> result = new ArrayList<>();
      String prefix = namespace + ":";
      for (Map.Entry<String, SecureStoreData> entry : store.entrySet()) {
        if (entry.getKey().startsWith(prefix)) {
          result.add(entry.getValue().getMetadata());
        }
      }
      return result;
    }

    @Override
    public SecureStoreData get(String namespace, String name) {
      return store.get(namespace + ":" + name);
    }

    @Override
    public void put(String namespace, String name, String data, @Nullable String description,
                    Map<String, String> properties) {
      SecureStoreMetadata meta = new SecureStoreMetadata(name, description, System.currentTimeMillis(), properties);
      store.put(namespace + ":" + name, new SecureStoreData(meta, data.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public void delete(String namespace, String name) {
      store.remove(namespace + ":" + name);
    }
  }
}
