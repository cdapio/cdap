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

package io.cdap.cdap.master.environment;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClientService;

import java.util.function.Supplier;

/**
 * A mock implementation of {@link MasterEnvironment} to facilitate unit testing.
 */
public class MockMasterEnvironment implements MasterEnvironment {

  private ZKClientService zkClient;
  private ZKDiscoveryService discoveryService;
  private TwillRunnerService twillRunnerService;

  @Override
  public void initialize(MasterEnvironmentContext context) {
    zkClient = ZKClientService.Builder.of(context.getConfigurations().get(Constants.Zookeeper.QUORUM)).build();
    zkClient.startAndWait();

    discoveryService = new ZKDiscoveryService(zkClient);
    twillRunnerService = new NoopTwillRunnerService();
  }

  @Override
  public void destroy() {
    discoveryService.close();
    zkClient.stopAndWait();
  }

  @Override
  public MasterEnvironmentRunnable createRunnable(MasterEnvironmentContext context,
                                                  Class<? extends MasterEnvironmentRunnable> cls) throws Exception {
    return cls.newInstance();
  }

  @Override
  public String getName() {
    return "mock";
  }

  @Override
  public Supplier<DiscoveryService> getDiscoveryServiceSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier() {
    return () -> discoveryService;
  }

  @Override
  public Supplier<TwillRunnerService> getTwillRunnerSupplier() {
    return () -> twillRunnerService;
  }


}
