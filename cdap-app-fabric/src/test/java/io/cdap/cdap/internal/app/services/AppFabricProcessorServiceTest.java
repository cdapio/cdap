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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.security.auth.AuditLogSubscriberService;
import java.lang.reflect.Field;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link AppFabricProcessorService}.
 */
public class AppFabricProcessorServiceTest {

  @Test
  public void startStopService() {
    try {
      Injector injector = AppFabricTestHelper.getInjector();
      AppFabricProcessorService service = injector.getInstance(AppFabricProcessorService.class);
      service.startAsync().awaitRunning();
      Service.State state = service.state();
      Assert.assertSame(state, Service.State.RUNNING);

      service.stopAsync().awaitTerminated();
      state = service.state();
      Assert.assertSame(state, Service.State.TERMINATED);
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }

  @Test
  public void startStopServiceWithAuditLogEnabled() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean("feature." + Feature.DATAPLANE_AUDIT_LOGGING.getFeatureFlagString(), true);
    AppFabricProcessorService service = null;
    try {
      Injector injector = AppFabricTestHelper.getInjector(cConf);
      service = injector.getInstance(AppFabricProcessorService.class);
      Field auditField =
          AppFabricProcessorService.class.getDeclaredField("auditLogSubscriberService");
      auditField.setAccessible(true);
      AuditLogSubscriberService auditLogService =
          (AuditLogSubscriberService) auditField.get(service);
      Field lifecycleField =
          AppFabricProcessorService.class.getDeclaredField("applicationLifecycleService");
      lifecycleField.setAccessible(true);
      ApplicationLifecycleService lifecycleService =
          (ApplicationLifecycleService) lifecycleField.get(service);

      service.startAsync().awaitRunning();
      Service.State state = service.state();
      Assert.assertSame(Service.State.RUNNING, state);
      Assert.assertSame(Service.State.RUNNING, auditLogService.state());
      Assert.assertSame(Service.State.RUNNING, lifecycleService.state());

      service.stopAsync().awaitTerminated();
      state = service.state();
      Assert.assertSame(Service.State.TERMINATED, state);
      Assert.assertSame(Service.State.TERMINATED, auditLogService.state());
      Assert.assertSame(Service.State.TERMINATED, lifecycleService.state());
    } finally {
      if (service != null && service.isRunning()) {
        service.stopAsync().awaitTerminated();
      }
      AppFabricTestHelper.shutdown();
    }
  }
}
