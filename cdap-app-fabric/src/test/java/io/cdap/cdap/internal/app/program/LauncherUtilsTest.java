/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.program;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.twill.api.TwillPreparer;
import org.junit.Test;
import org.mockito.Matchers;

/**
 * Tests for {@link LauncherUtils}.
 */
public class LauncherUtilsTest {
  @Test
  public void testOverrideJVMOpts() {
    String testRunnableName = "test-runnable";
    String testJVMOpts = "-DtestOptions";
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.AppFabric.PROGRAM_JVM_OPTS + "." + testRunnableName, testJVMOpts);
    TwillPreparer mockTwillPreparer = mock(TwillPreparer.class);
    Set<String> testRunnables = Collections.singleton(testRunnableName);
    LauncherUtils.overrideJVMOpts(cConf, mockTwillPreparer, testRunnables);
    verify(mockTwillPreparer, times(1)).setJVMOptions(testRunnableName, testJVMOpts);
  }

  @Test
  public void testOverrideJVMOptsDifferentRunnable() {
    String testRunnableName = "test-runnable";
    String testJVMOpts = "-DtestOptions";
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.AppFabric.PROGRAM_JVM_OPTS + "." + testRunnableName, testJVMOpts);
    TwillPreparer mockTwillPreparer = mock(TwillPreparer.class);
    Set<String> testRunnables = Collections.singleton("other-test-runnable");
    LauncherUtils.overrideJVMOpts(cConf, mockTwillPreparer, testRunnables);
    verify(mockTwillPreparer, never()).setJVMOptions(Matchers.any(), Matchers.any());
  }

  @Test
  public void testOverrideJVMOptsNoConfig() {
    String testRunnableName = "test-runnable-2";
    String testJVMOpts = "-DtestOptions";
    CConfiguration cConf = CConfiguration.create();
    TwillPreparer mockTwillPreparer = mock(TwillPreparer.class);
    Set<String> testRunnables = Collections.singleton(testRunnableName);
    LauncherUtils.overrideJVMOpts(cConf, mockTwillPreparer, testRunnables);
    verify(mockTwillPreparer, never()).setJVMOptions(Matchers.any(), Matchers.any());
  }

  @Test
  public void testOverrideJVMOptsMultiplerunnables() {
    String testRunnableName1 = "test-runnable-1";
    String testJVMOpts1 = "-DtestOptions";
    String testRunnableName2 = "test-runnable-2";
    String testJVMOpts2 = "-DtestOptions -DsomeOtherTestOptions";
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.AppFabric.PROGRAM_JVM_OPTS + "." + testRunnableName1, testJVMOpts1);
    cConf.set(Constants.AppFabric.PROGRAM_JVM_OPTS + "." + testRunnableName2, testJVMOpts2);
    TwillPreparer mockTwillPreparer = mock(TwillPreparer.class);
    Set<String> testRunnables = new HashSet<>();
    testRunnables.add(testRunnableName1);
    testRunnables.add(testRunnableName2);
    LauncherUtils.overrideJVMOpts(cConf, mockTwillPreparer, testRunnables);
    verify(mockTwillPreparer, times(1)).setJVMOptions(testRunnableName1, testJVMOpts1);
    verify(mockTwillPreparer, times(1)).setJVMOptions(testRunnableName2, testJVMOpts2);
  }
}
