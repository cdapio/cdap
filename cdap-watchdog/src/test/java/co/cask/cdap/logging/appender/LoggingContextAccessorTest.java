/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.logging.context.GenericLoggingContext;
import org.apache.twill.common.Cancellable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.MDC;

/**
 * Unit test for {@link LoggingContextAccessor}
 */
public class LoggingContextAccessorTest {
  private static final String OLD_NS = "testNamespaceOld";
  private static final String OLD_APP = "testAppOld";
  private static final String OLD_ENTITY = "testEntityOld";
  private static final String NS = "testNamespace";
  private static final String APP = "testApp";
  private static final String ENTITY = "testEntity";

  @Test
  public void testReset() {
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(
      new GenericLoggingContext(OLD_NS, OLD_APP, OLD_ENTITY));
    Assert.assertEquals(MDC.get(NamespaceLoggingContext.TAG_NAMESPACE_ID), OLD_NS);
    Assert.assertEquals(MDC.get(ApplicationLoggingContext.TAG_APPLICATION_ID), OLD_APP);
    Assert.assertEquals(MDC.get(GenericLoggingContext.TAG_ENTITY_ID), OLD_ENTITY);

    final Cancellable cancellable2 =
      LoggingContextAccessor.setLoggingContext(new GenericLoggingContext(NS, APP, ENTITY));

    Assert.assertEquals(MDC.get(NamespaceLoggingContext.TAG_NAMESPACE_ID), NS);
    Assert.assertEquals(MDC.get(ApplicationLoggingContext.TAG_APPLICATION_ID), APP);
    Assert.assertEquals(MDC.get(GenericLoggingContext.TAG_ENTITY_ID), ENTITY);

    // Verify a different thread cannot change context
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        cancellable2.cancel();
      }
    });
    thread.start();
    try {
      thread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Assert.assertEquals(MDC.get(NamespaceLoggingContext.TAG_NAMESPACE_ID), NS);
    Assert.assertEquals(MDC.get(ApplicationLoggingContext.TAG_APPLICATION_ID), APP);
    Assert.assertEquals(MDC.get(GenericLoggingContext.TAG_ENTITY_ID), ENTITY);

    // Reset in the same thread
    cancellable2.cancel();

    Assert.assertEquals(MDC.get(NamespaceLoggingContext.TAG_NAMESPACE_ID), OLD_NS);
    Assert.assertEquals(MDC.get(ApplicationLoggingContext.TAG_APPLICATION_ID), OLD_APP);
    Assert.assertEquals(MDC.get(GenericLoggingContext.TAG_ENTITY_ID), OLD_ENTITY);

    // Check reset back to nothing
    cancellable.cancel();
    Assert.assertTrue(MDC.getCopyOfContextMap().isEmpty());
  }
}
