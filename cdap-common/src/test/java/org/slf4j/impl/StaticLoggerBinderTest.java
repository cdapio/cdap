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

package org.slf4j.impl;

import ch.qos.logback.classic.LoggerContext;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for the {@link StaticLoggerBinder} compatibility shim.
 */
public class StaticLoggerBinderTest {

  @Test
  public void testBinderMatchesActiveLoggerFactory() {
    StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
    Assert.assertNotNull(binder);
    Assert.assertSame(LoggerFactory.getILoggerFactory(), binder.getLoggerFactory());
    Assert.assertTrue(binder.getLoggerFactory() instanceof LoggerContext);
    Assert.assertEquals(LoggerContext.class.getName(), binder.getLoggerFactoryClassStr());
  }
}
