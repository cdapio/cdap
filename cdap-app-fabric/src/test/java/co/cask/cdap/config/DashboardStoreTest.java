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
 * Tests for {@link DashboardStore}
 */
public class DashboardStoreTest extends AppFabricTestBase {

  @Test
  public void testDeletingNamespace() throws Exception {
    String namespace = "myspace";
    int dashboardCount = 10;
    Map<String, String> emptyMap = ImmutableMap.of();
    DashboardStore store = getInjector().getInstance(DashboardStore.class);
    for (int i = 0; i < dashboardCount; i++) {
      store.create(namespace, emptyMap);
    }

    Assert.assertEquals(dashboardCount, store.list(namespace).size());
    store.delete(namespace);
    Assert.assertTrue(store.list(namespace).isEmpty());
  }
}
