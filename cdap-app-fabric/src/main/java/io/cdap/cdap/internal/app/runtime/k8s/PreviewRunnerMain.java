/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.k8s;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.master.environment.k8s.AbstractServiceMain;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;


/**
 * The main class to run the preview runner. Preview runner will run in its own pod.
 */
public class PreviewRunnerMain extends AbstractServiceMain<PreviewRunnerOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    System.out.println("In the preview runner main");
    Thread.sleep(120000);
    System.out.println("Preview runner main done");
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv, PreviewRunnerOptions options) {
    return Collections.emptyList();
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources, MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext, PreviewRunnerOptions options) {

  }

  @Nullable
  @Override
  protected LoggingContext getLoggingContext(PreviewRunnerOptions options) {
    return null;
  }
}
