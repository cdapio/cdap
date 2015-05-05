/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.config;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for {@link ConsoleSettingsStore}
 */
public class ConsoleSettingsStoreTest extends AppFabricTestBase {

  @Test
  public void testDeletingConfigs() throws Exception {
    ConsoleSettingsStore store = getInjector().getInstance(ConsoleSettingsStore.class);
    int configCount = 10;
    Map<String, String> emptyMap = ImmutableMap.of();
    for (int i = 0; i < configCount; i++) {
      store.put(new Config(String.valueOf(i), emptyMap));
    }

    Assert.assertEquals(configCount, store.list().size());
    store.delete();
    Assert.assertTrue(store.list().isEmpty());
  }
}
