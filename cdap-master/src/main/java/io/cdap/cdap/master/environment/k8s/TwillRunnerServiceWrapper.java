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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.TwillRunnerService;

/**
 * A Guava {@link Service} that wraps the {@link TwillRunnerService#start()} and {@link TwillRunnerService#stop()}
 * calls.
 */
final class TwillRunnerServiceWrapper extends AbstractIdleService {

  private final TwillRunnerService twillRunner;

  TwillRunnerServiceWrapper(TwillRunnerService twillRunner) {
    this.twillRunner = twillRunner;
  }

  @Override
  protected void startUp() {
    twillRunner.start();
  }

  @Override
  protected void shutDown() {
    twillRunner.stop();
  }
}
