/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.guice.AppFabricTestModule;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.DefaultNamespaceStore;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DistributedProgramRunnerTest {

  private static final String NAMESPACE = "namespace";
  private static final Map<String, String> CONFIGS = Collections.singletonMap("property_1", "value_1");

  private static DefaultNamespaceStore nsStore;
  private static DistributedProgramRunner distributedProgramRunner;
  private static NamespaceQueryAdmin namespaceQueryAdmin;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new AppFabricTestModule(CConfiguration.create()));
    nsStore = new DefaultNamespaceStore(injector.getInstance(TransactionRunner.class));
    namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    distributedProgramRunner = new DistributedServiceProgramRunner(cConf, null, null, ClusterMode.ON_PREMISE, null,
                                                                   injector);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testGetNamespaceConfigs() throws Exception {
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(NAMESPACE).setConfig(CONFIGS).build();
    nsStore.create(meta);

    Map<String, String> foundNamespaceConfigs = distributedProgramRunner.getNamespaceConfigs(NAMESPACE);
    Assert.assertTrue(foundNamespaceConfigs.entrySet().containsAll(CONFIGS.entrySet()));
  }
}
