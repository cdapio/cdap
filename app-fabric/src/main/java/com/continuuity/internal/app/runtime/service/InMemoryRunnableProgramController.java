/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.runtime.service;

import com.continuuity.internal.app.runtime.AbstractProgramController;
import org.apache.twill.api.TwillContext;

/**
 * Program Controller for Service runnable
 */
final class InMemoryRunnableProgramController extends AbstractProgramController {
  private InMemoryRunnableDriver driver;

  InMemoryRunnableProgramController(String serviceName, String runnableName,
                                    TwillContext twillContext, InMemoryRunnableDriver driver) {
    super(serviceName + ":" + runnableName, twillContext.getRunId());
    this.driver = driver;
  }

  @Override
  protected void doSuspend() throws Exception {
    //no-op
  }

  @Override
  protected void doResume() throws Exception {
    //no-op
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    //no-op
  }
}
