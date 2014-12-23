/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.config.ConfigService;
import co.cask.cdap.app.config.ConfigType;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.test.internal.guice.AppFabricTestModule;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test for {@link ConfigService}.
 */
public class ConfigServiceTest {
  private static TransactionManager txManager;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static ConfigService dashboardService;
  private static ConfigService userSettingsService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CConfiguration conf = CConfiguration.create();
    Injector injector = Guice.createInjector(new AppFabricTestModule(conf));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    dashboardService = injector.getInstance(Key.get(ConfigService.class,
                                                    Names.named(Constants.ConfigService.DASHBOARD)));
    dashboardService.startAndWait();
    userSettingsService = injector.getInstance(Key.get(ConfigService.class,
                                                       Names.named(Constants.ConfigService.USERSETTING)));
    userSettingsService.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    dashboardService.stopAndWait();
    userSettingsService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txManager.stopAndWait();
  }

  @Test
  public void testUISettings() throws Exception {
    String namespace = "userspace";
    String userId = "id";
    ConfigType type = ConfigType.USER;
    List<String> configs = userSettingsService.getConfig(namespace, type, userId);
    Assert.assertEquals(0, configs.size());

    userSettingsService.writeSetting(namespace, type, userId, "k1", "v1");
    Map<String, String> settings = userSettingsService.readSetting(namespace, type, userId);
    Assert.assertEquals(1, settings.size());
    Assert.assertEquals("v1", settings.get("k1"));

    userSettingsService.deleteConfig(namespace, type, userId, userId);
    settings = userSettingsService.readSetting(namespace, type, userId);
    Assert.assertEquals(0, settings.size());
  }

  @Test
  public void testDashboard() throws Exception {
    String namespace = "myspace";
    ConfigType type = ConfigType.DASHBOARD;
    List<String> configs = dashboardService.getConfig(namespace, type, "myId");
    Assert.assertEquals(0, configs.size());

    String dashId = dashboardService.createConfig(namespace, type, "user1");

    configs = dashboardService.getConfig(namespace, type, "user1");
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals(dashId, configs.get(0));

    configs = dashboardService.getConfig(namespace, type, "myId");
    Assert.assertEquals(0, configs.size());
    configs = dashboardService.getConfig(namespace, type);
    Assert.assertEquals(1, configs.size());
    Assert.assertEquals(dashId, configs.get(0));

    Map<String, String> settings = dashboardService.readSetting(namespace, type, dashId);
    Assert.assertEquals(0, settings.size());
    String value = dashboardService.readSetting(namespace, type, dashId, "key");
    Assert.assertEquals(null, value);

    dashboardService.writeSetting(namespace, type, dashId, "key", "value");
    value = dashboardService.readSetting(namespace, type, dashId, "key");
    Assert.assertEquals("value", value);

    Map<String, String> newSettings = Maps.newHashMap();
    newSettings.put("key", "noval");
    newSettings.put("k1", "v1");
    dashboardService.writeSetting(namespace, type, dashId, newSettings);
    settings = dashboardService.readSetting(namespace, type, dashId);
    Assert.assertEquals(2, settings.size());
    Assert.assertEquals("noval", settings.get("key"));
    Assert.assertEquals("v1", settings.get("k1"));

    dashboardService.deleteSetting(namespace, type, dashId, "k1");
    Assert.assertEquals(null, dashboardService.readSetting(namespace, type, dashId, "k1"));
    Assert.assertEquals(1, dashboardService.readSetting(namespace, type, dashId).size());

    dashboardService.deleteSetting(namespace, type, dashId);
    Assert.assertEquals(0, dashboardService.readSetting(namespace, type, dashId).size());

    Assert.assertEquals(true, dashboardService.checkConfig(namespace, type, dashId));

    dashboardService.deleteConfig(namespace, type, "user1", dashId);
    configs = dashboardService.getConfig(namespace, type);
    Assert.assertEquals(0, configs.size());
  }
}
