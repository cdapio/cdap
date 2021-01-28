/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.common.macro;

import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

/**
 *
 */
public class LogicalStartTimeMacroEvaluatorTest {

  @Test
  public void testSubstitution() {
    long logicalStartTime = 0;
    LogicalStartTimeMacroEvaluator runtimeMacro = new LogicalStartTimeMacroEvaluator(logicalStartTime,
                                                                                     TimeZone.getTimeZone("UTC"));
    String functionName = LogicalStartTimeMacroEvaluator.FUNCTION_NAME;

    Assert.assertEquals("0", runtimeMacro.evaluate(functionName, ""));
    Assert.assertEquals("1970-01-01", runtimeMacro.evaluate(functionName, "yyyy-MM-dd"));
    Assert.assertEquals("1969-12-31", runtimeMacro.evaluate(functionName, "yyyy-MM-dd", "1d"));
    Assert.assertEquals("1969-12-31", runtimeMacro.evaluate(functionName, "yyyy-MM-dd", "+1d"));
    Assert.assertEquals("1970-01-02", runtimeMacro.evaluate(functionName, "yyyy-MM-dd", "-1d"));

    logicalStartTime = 1451606400000L;
    runtimeMacro = new LogicalStartTimeMacroEvaluator(logicalStartTime, TimeZone.getTimeZone("UTC"));
    Assert.assertEquals("2016-01-01", runtimeMacro.evaluate(functionName, "yyyy-MM-dd"));
    Assert.assertEquals("2015-12-31", runtimeMacro.evaluate(functionName, "yyyy-MM-dd", "1d"));
    Assert.assertEquals("2015-12-31T00:00:00", runtimeMacro.evaluate(functionName,
                                                                     "yyyy-MM-dd'T'HH:mm:ss", "1d"));
    Assert.assertEquals("2015-12-31T12:00:00", runtimeMacro.evaluate(functionName,
                                                                     "yyyy-MM-dd'T'HH:mm:ss", "1d-12h"));
    Assert.assertEquals("2015-12-31T11:29:45",
                        runtimeMacro.evaluate(functionName,
                                              "yyyy-MM-dd'T'HH:mm:ss", "1d-12h+30m+15s"));
  }

  @Test
  public void testOffset() {
    LogicalStartTimeMacroEvaluator runtimeMacro = new LogicalStartTimeMacroEvaluator(0, TimeZone.getTimeZone("UTC"));
    Assert.assertEquals("1969-12-31T23:30:00", runtimeMacro.evaluate(LogicalStartTimeMacroEvaluator.FUNCTION_NAME,
                                                                     "yyyy-MM-dd'T'HH:mm:ss", "30m"));
  }

  @Test
  public void testTimeZone() {
    LogicalStartTimeMacroEvaluator runtimeMacro = new LogicalStartTimeMacroEvaluator(0, TimeZone.getTimeZone("PST"));
    Assert.assertEquals("1969-12-31T15:30:00", runtimeMacro.evaluate(LogicalStartTimeMacroEvaluator.FUNCTION_NAME,
                                                                     "yyyy-MM-dd'T'HH:mm:ss", "30m", "PST "));
  }

}
