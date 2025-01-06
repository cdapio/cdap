/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.internal.AppFabricTestHelper;
import org.junit.Assert;
import org.junit.Test;

public class AppFabricProcessorServiceTest {

  @Test
  public void startStopService() {
    try {
      Injector injector = AppFabricTestHelper.getInjector();
      AppFabricProcessorService service = injector.getInstance(AppFabricProcessorService.class);
      Service.State state = service.startAndWait();
      Assert.assertSame(state, Service.State.RUNNING);

      state = service.stopAndWait();
      Assert.assertSame(state, Service.State.TERMINATED);
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }
}
