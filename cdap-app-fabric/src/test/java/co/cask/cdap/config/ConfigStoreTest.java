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
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link ConfigStore}
 */
public class ConfigStoreTest extends AppFabricTestBase {

  @Test
  public void testSimpleConfig() throws Exception {
    String namespace = "myspace";
    String type = "dash";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    Config myConfig = new Config("abcd", ImmutableMap.<String, String>of());
    configStore.create(namespace, type, myConfig);
    Assert.assertEquals(myConfig, configStore.get(namespace, type, myConfig.getId()));
    List<Config> configList = configStore.list(namespace, type);
    Assert.assertEquals(1, configList.size());
    Assert.assertEquals(myConfig, configList.get(0));

    Map<String, String> properties = Maps.newHashMap();
    properties.put("hello", "world");
    properties.put("config", "store");
    myConfig = new Config("abcd", properties);

    configStore.update(namespace, type, myConfig);
    Assert.assertEquals(myConfig, configStore.get(namespace, type, myConfig.getId()));
    configStore.delete(namespace, type, myConfig.getId());
    configList = configStore.list(namespace, type);
    Assert.assertEquals(0, configList.size());
  }

  @Test(expected = ConfigExistsException.class)
  public void testDuplicateConfig() throws Exception {
    String namespace = "space";
    String type = "user";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    Config myConfig = new Config("abcd", ImmutableMap.<String, String>of());
    configStore.create(namespace, type, myConfig);
    Assert.assertEquals(1, configStore.list(namespace, type).size());
    configStore.create(namespace, type, myConfig);
  }

  @Test(expected = ConfigNotFoundException.class)
  public void testDeleteUnknownConfig() throws Exception {
    String namespace = "newspace";
    String type = "prefs";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    configStore.delete(namespace, type, "someid");
  }

  @Test(expected = ConfigNotFoundException.class)
  public void testGetUnknownConfig() throws Exception {
    String namespace = "newspace";
    String type = "prefs";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    configStore.get(namespace, type, "someid");
  }

  @Test(expected = ConfigNotFoundException.class)
  public void testUpdateUnknownConfig() throws Exception {
    String namespace = "newspace";
    String type = "prefs";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    Config myConfig = new Config("abcd", ImmutableMap.<String, String>of());
    configStore.update(namespace, type, myConfig);
  }

  @Test
  public void testRandomData() throws Exception {
    String namespace = "somesp@#ace123!@";
    String type = "s231!@#";
    String id = "kj324";
    ConfigStore configStore = getInjector().getInstance(ConfigStore.class);
    Map<String, String> prop = Maps.newHashMap();
    prop.put("j342", "9834@#($");
    prop.put("123jsd123@#", "????213");
    Config myConfig = new Config(id, prop);
    configStore.create(namespace, type, myConfig);
    Assert.assertEquals(1, configStore.list(namespace, type).size());
    Assert.assertEquals(myConfig, configStore.get(namespace, type, id));
    configStore.delete(namespace, type, id);
    Assert.assertEquals(0, configStore.list(namespace, type).size());
  }
}
