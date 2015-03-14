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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;

import java.util.concurrent.TimeUnit;

/**
 * Base class for scheduler tests
 */
public class SchedulerTestBase {
  protected void waitUntilFinished(ProgramRuntimeService runtimeService,
                                   Id.Program program, int maxWaitSeconds) throws InterruptedException {
    int retries = 0;
    while (isProgramRunning(runtimeService, program) && retries < maxWaitSeconds) {
      TimeUnit.SECONDS.sleep(1);
      retries++;
    }
  }

  private boolean isProgramRunning(ProgramRuntimeService runtimeService, final Id.Program program) {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.equals(program);
      }
    }, ProgramType.values());
  }
}
