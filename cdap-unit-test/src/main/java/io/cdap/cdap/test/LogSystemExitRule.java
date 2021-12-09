/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.test;

import org.junit.rules.ExternalResource;

import java.security.Permission;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 *   For any {@link System#exit(int)} call logs it to the stderr along with stack trace.
 *   This rule is useful to catch cases with
 *   "The forked VM terminated without properly saying goodbye. VM crash or System.exit called?"
 *   Maven surefire error.
 *   Note that this does not prevent System.exit to allow handling various corner cases, just logs it.
 * </p>
 * <p>
 *   The way to use it:
 * </p>
 * <p>{@code
 *   @ClassRule
 *   public static final LogSystemExitRule SYSTEM_EXIT_RULE = LogSystemExitRule.get();
 * }</p>
 */
public final class LogSystemExitRule extends ExternalResource {
  private static final LogSystemExitRule INSTANCE = new LogSystemExitRule();

  public static LogSystemExitRule get() {
    return INSTANCE;
  }

  private AtomicReference<SecurityManager> originalSecurityManager = new AtomicReference<>();
  private AtomicLong testsRunning = new AtomicLong();
  private final SecurityManager securityManager = new SecurityManager() {
    @Override
    public void checkPermission(Permission permission) {
      if (permission.getName() != null && permission.getName().startsWith("exitVM.")) {
        new IllegalStateException("ERROR: System.exit called: " + permission).printStackTrace();
      }
    }
  };

  private LogSystemExitRule() {

  }

  @Override
  protected synchronized void before() throws Throwable {
    if (testsRunning.getAndIncrement() == 0) {
      originalSecurityManager.set(System.getSecurityManager());
      System.setSecurityManager(securityManager);
    }
  }

  @Override
  protected synchronized void after() {
    if (testsRunning.decrementAndGet() == 0) {
      System.setSecurityManager(originalSecurityManager.get());
    }
  }
}
