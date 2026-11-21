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

import java.security.Security;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractProgramTwillRunnable#initializeDnsCachePolicy()}.
 */
public class AbstractProgramTwillRunnableTest {

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
  public void testDefaultsToThirtySecondsWhenUnset() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("30",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testDefaultsWhenSecurityPropertyIsBlank() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "  ");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("30",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesPreSetSecurityProperty() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "60");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("60",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesPreSetSystemProperty() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "");
    System.setProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL, "45");

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesExplicitForeverPolicy() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "-1");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("-1",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }
}
