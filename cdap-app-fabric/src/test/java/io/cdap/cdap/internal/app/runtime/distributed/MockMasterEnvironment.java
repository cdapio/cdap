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

import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import java.util.function.Supplier;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.InMemoryDiscoveryService;

/**
 * A mock implementation of {@link MasterEnvironment} for unit testing program twill runnables.
 */
public final class MockMasterEnvironment implements MasterEnvironment {

  private final InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();

  @Override
  public MasterEnvironmentRunnable createRunnable(
    MasterEnvironmentRunnableContext context,
    Class<? extends MasterEnvironmentRunnable> runnableClass) throws Exception {
    throw new UnsupportedOperationException();
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
    return NoopTwillRunnerService::new;
  }
}
