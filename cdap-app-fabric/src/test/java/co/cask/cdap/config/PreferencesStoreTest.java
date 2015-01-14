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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for {@link PreferencesStore}
 */
public class PreferencesStoreTest extends AppFabricTestBase {

  @Test
  public void testCleanSlate() throws Exception {
    Map<String, String> emptyMap = ImmutableMap.of();
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    Assert.assertEquals(emptyMap, store.getProperties());
    Assert.assertEquals(emptyMap, store.getProperties("somenamespace"));
    Assert.assertEquals(emptyMap, store.getProperties(Constants.DEFAULT_NAMESPACE));
    Assert.assertEquals(emptyMap, store.getResolvedProperties());
    Assert.assertEquals(emptyMap, store.getResolvedProperties("a", "b", "c", "d"));
    // should not throw any exception if try to delete properties without storing anything
    store.deleteProperties();
    store.deleteProperties(Constants.DEFAULT_NAMESPACE);
    store.deleteProperties("a", "x", "y", "z");
  }

  @Test
  public void testBasicProperties() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "instance");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties(propMap);
    Assert.assertEquals(propMap, store.getProperties());
    Assert.assertEquals(propMap, store.getResolvedProperties("a", "b", "c", "d"));
    Assert.assertEquals(propMap, store.getResolvedProperties("myspace"));
    Assert.assertEquals(ImmutableMap.<String, String>of(), store.getProperties("myspace"));
    store.deleteProperties();
    propMap.clear();
    Assert.assertEquals(propMap, store.getProperties());
    Assert.assertEquals(propMap, store.getResolvedProperties("a", "b", "c", "d"));
    Assert.assertEquals(propMap, store.getResolvedProperties("myspace"));
  }

  @Test
  public void testMultiLevelProperties() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    propMap.put("key", "namespace");
    PreferencesStore store = getInjector().getInstance(PreferencesStore.class);
    store.setProperties("myspace", propMap);
    propMap.put("key", "application");
    store.setProperties("myspace", "app", propMap);
    Assert.assertEquals(propMap, store.getProperties("myspace", "app"));
    Assert.assertEquals("namespace", store.getProperties("myspace").get("key"));
    Assert.assertTrue(store.getProperties("myspace", "notmyapp").isEmpty());
    Assert.assertEquals("namespace", store.getResolvedProperties("myspace", "notmyapp").get("key"));
    Assert.assertTrue(store.getProperties("notmyspace").isEmpty());
    store.deleteProperties("myspace");
    Assert.assertTrue(store.getProperties("myspace").isEmpty());
    Assert.assertTrue(store.getResolvedProperties("myspace", "notmyapp").isEmpty());
    Assert.assertEquals(propMap, store.getProperties("myspace", "app"));
    store.deleteProperties("myspace", "app");
    Assert.assertTrue(store.getProperties("myspace", "app").isEmpty());
    propMap.put("key", "program");
    store.setProperties("myspace", "app", "type", "prog", propMap);
    Assert.assertEquals(propMap, store.getProperties("myspace", "app", "type", "prog"));
    store.setProperties(ImmutableMap.of("key", "instance"));
    Assert.assertEquals(propMap, store.getProperties("myspace", "app", "type", "prog"));
    store.deleteProperties("myspace", "app", "type", "prog");
    Assert.assertTrue(store.getProperties("myspace", "app", "type", "prog").isEmpty());
    Assert.assertEquals("instance", store.getResolvedProperties("myspace", "app", "type", "prog").get("key"));
    store.deleteProperties();
    Assert.assertEquals(ImmutableMap.<String, String>of(), store.getProperties("myspace", "app", "type", "prog"));
  }
}
