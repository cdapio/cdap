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

package io.cdap.cdap.internal.app.dispatcher;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link TaskWorkerService}.
 */
public class TaskWorkerServiceTest {

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 200);
    return cConf;
  }

  @Test
  public void testStartAndStop() throws InterruptedException, ExecutionException, TimeoutException {
    TaskWorkerService taskWorkerService = new TaskWorkerService(createCConf());
    taskWorkerService.startAndWait();
    taskWorkerService.stopAndWait();
  }
}