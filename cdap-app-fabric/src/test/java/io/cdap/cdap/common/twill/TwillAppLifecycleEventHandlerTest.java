/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link TwillAppLifecycleEventHandler}.
 */
public class TwillAppLifecycleEventHandlerTest {

  @Test
  public void testDestroyWhenUninitializedIsSafe() {
    TwillAppLifecycleEventHandler handler = new TwillAppLifecycleEventHandler(1000L, false);
    handler.destroy();
  }

  @Test
  public void testDestroyClosesLogAppenderQuietlyOnIoExceptionAndStopsZkService() throws Exception {
    TwillAppLifecycleEventHandler handler = new TwillAppLifecycleEventHandler(1000L, false);

    AtomicBoolean logClosed = new AtomicBoolean(false);
    LogAppenderInitializer logInitializer = Mockito.mock(LogAppenderInitializer.class);
    Mockito.doAnswer(invocation -> {
      logClosed.set(true);
      throw new IOException("close io failure");
    }).when(logInitializer).close();

    Service backingService = new AbstractIdleService() {
      @Override
      protected void startUp() {
        // no-op
      }

      @Override
      protected void shutDown() {
        // no-op
      }
    };
    backingService.startAndWait();

    ZKClientService zkClientService = Mockito.mock(ZKClientService.class, invocation -> {
      String methodName = invocation.getMethod().getName();
      if ("stop".equals(methodName)) {
        return backingService.stop();
      }
      if ("stopAsync".equals(methodName)) {
        backingService.stop();
        return invocation.getMock();
      }
      if ("state".equals(methodName)) {
        return backingService.state();
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    });

    setField(handler, "logAppenderInitializer", logInitializer);
    setField(handler, "zkClientService", zkClientService);

    handler.destroy();

    Assert.assertTrue(logClosed.get());
    Tasks.waitFor(Service.State.TERMINATED, backingService::state, 5, TimeUnit.SECONDS,
        20, TimeUnit.MILLISECONDS);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitializeThrowsWhenConfFileMissing() {
    TwillAppLifecycleEventHandler handler = new TwillAppLifecycleEventHandler(1000L, false);
    EventHandlerContext context =
        Mockito.mock(EventHandlerContext.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(context.getRunId()).thenReturn(RunIds.generate());
    handler.initialize(context);
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = TwillAppLifecycleEventHandler.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
