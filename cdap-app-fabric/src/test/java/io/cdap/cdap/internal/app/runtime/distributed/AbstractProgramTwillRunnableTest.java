/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.security.Security;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link AbstractProgramTwillRunnable#initializeDnsCachePolicy()}.
 */
public class AbstractProgramTwillRunnableTest {

  private static final String DEFAULT_TTL =
      AbstractProgramTwillRunnable.DEFAULT_DNS_CACHE_TTL_SECONDS;

  private String originalSecurityTtl;
  private String originalSystemTtl;

  @Before
  public void setUp() {
    originalSecurityTtl = Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL);
    originalSystemTtl = System.getProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);
  }

  @After
  public void tearDown() {
    Security.setProperty(
        AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL,
        originalSecurityTtl == null ? "" : originalSecurityTtl);
    if (originalSystemTtl == null) {
      System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);
    } else {
      System.setProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL, originalSystemTtl);
    }
  }

  @Test
  public void testDefaultsWhenNoValidTtlIsSet() {
    // Security property "" stands for unset, since a security property cannot be removed.
    assertSecurityTtlAfterInit("", null, DEFAULT_TTL);
    assertSecurityTtlAfterInit("  ", null, DEFAULT_TTL);
    // Values that are not integers are ignored by the JDK, which then caches forever once a
    // SecurityManager is installed. They must get the default instead.
    assertSecurityTtlAfterInit("invalid", null, DEFAULT_TTL);
    assertSecurityTtlAfterInit("", "invalid", DEFAULT_TTL);
    // System.getProperty() does not trim, so the JDK cannot parse a padded system property.
    assertSecurityTtlAfterInit("", " 60 ", DEFAULT_TTL);
  }

  @Test
  public void testPreservesValidTtl() {
    assertSecurityTtlAfterInit("60", null, "60");
    // Security.getProperty() trims, so the JDK parses a padded security property.
    assertSecurityTtlAfterInit(" 60 ", null, " 60 ");
    assertSecurityTtlAfterInit("0", null, "0");
    // A valid system property is used by the JDK, so the security property is left alone.
    assertSecurityTtlAfterInit("", "45", "");
    assertSecurityTtlAfterInit("", "0x3c", "");
    assertSecurityTtlAfterInit("invalid", "45", "invalid");
  }

  @Test
  public void testPreservesAndWarnsOnForeverPolicy() {
    Logger logger = (Logger) LoggerFactory.getLogger(AbstractProgramTwillRunnable.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      assertSecurityTtlAfterInit("-1", null, "-1");
      assertSecurityTtlAfterInit("", "-5", "");
      Assert.assertEquals(2, countForeverWarnings(appender));

      appender.list.clear();
      assertSecurityTtlAfterInit("60", null, "60");
      assertSecurityTtlAfterInit("invalid", null, DEFAULT_TTL);
      Assert.assertEquals(0, countForeverWarnings(appender));
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  private static void assertSecurityTtlAfterInit(String securityTtl, @Nullable String systemTtl,
      String expectedSecurityTtl) {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, securityTtl);
    if (systemTtl == null) {
      System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);
    } else {
      System.setProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL, systemTtl);
    }

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();

    // Security.getProperty() trims, so compare against the trimmed expected value.
    Assert.assertEquals(
        String.format("securityTtl='%s', systemTtl='%s'", securityTtl, systemTtl),
        expectedSecurityTtl.trim(),
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  private static long countForeverWarnings(ListAppender<ILoggingEvent> appender) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("FOREVER"))
        .count();
  }
}
