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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.app.preview.PreviewRunner;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;

/**
 * Wraps a {@link PreviewRunner} to be run via Twill.
 */
public class PreviewRunnerTwillRunnable implements TwillRunnable {
  private static final String NAME = "preview-runner";

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(NAME)
      .withConfigs(ImmutableMap.of("KUBE_RESOURCE_TYPE", "STATEFULSET"))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {

  }

  @Override
  public void handleCommand(Command command) throws Exception {

  }

  @Override
  public void stop() {

  }

  @Override
  public void destroy() {

  }

  @Override
  public void run() {

  }
}
