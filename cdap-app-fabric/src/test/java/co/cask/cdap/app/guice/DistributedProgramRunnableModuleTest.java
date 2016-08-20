/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.inject.Guice;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.locks.Lock;

/**
 * Test case that simple creates a Guice injector from the {@link DistributedProgramRunnableModule}, to ensure
 * that all bindings are proper.
 */
public class DistributedProgramRunnableModuleTest {
  @Test
  public void createModule() throws Exception {
    DistributedProgramRunnableModule distributedProgramRunnableModule =
      new DistributedProgramRunnableModule(CConfiguration.create(), new Configuration());
    Guice.createInjector(distributedProgramRunnableModule.createModule());
    Guice.createInjector(distributedProgramRunnableModule.createModule(new TwillContext() {
      @Override
      public RunId getRunId() {
        return null;
      }

      @Override
      public RunId getApplicationRunId() {
        return null;
      }

      @Override
      public int getInstanceCount() {
        return 0;
      }

      @Override
      public InetAddress getHost() {
        // used by DistributedProgramRunnableModule#createModule(TwillContext)
        return new InetSocketAddress("localhost", 0).getAddress();
      }

      @Override
      public String[] getArguments() {
        return new String[0];
      }

      @Override
      public String[] getApplicationArguments() {
        return new String[0];
      }

      @Override
      public TwillRunnableSpecification getSpecification() {
        return null;
      }

      @Override
      public int getInstanceId() {
        return 0;
      }

      @Override
      public int getVirtualCores() {
        return 0;
      }

      @Override
      public int getMaxMemoryMB() {
        return 0;
      }

      @Override
      public ServiceDiscovered discover(String name) {
        return null;
      }

      @Override
      public Cancellable electLeader(String name, ElectionHandler participantHandler) {
        return null;
      }

      @Override
      public Lock createLock(String name) {
        return null;
      }

      @Override
      public Cancellable announce(String serviceName, int port) {
        return null;
      }
    }));
  }
}
