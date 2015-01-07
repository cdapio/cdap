/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Test for {@link ArgumentParser}.
 */
public class ArgumentParserTest {

  @Test
  public void basicTest1() {
    Map<String, String> expected = Maps.newHashMap();
    expected.put("param1", "some input1");
    expected.put("param2", "some input2");
    expected.put("param3", "some input3");
    String pattern = "test command <param1> and <param2> with <param3>";
    String input = "test command \"some input1\" and 'some input2' with \"some input3\" ";

    Map<String, String> actual = ArgumentParser.getArguments(input, pattern);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void basicTest2() {
    Map<String, String> expected = Maps.newHashMap();
    expected.put("param1", "some input1");
    String pattern = "test command <param1>";
    String input = "test command \"some input1\" and 'some input2' ";

    Map<String, String> actual = ArgumentParser.getArguments(input, pattern);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNotFullyEnteredLastParam() {
    Map<String, String> expected = Maps.newHashMap();
    expected.put("param1", "input1");
    expected.put("param2", "input2");
    String pattern = "test command <param1> and <param2> with <param3>";
    String input = "test command input1 and 'input2' with notFullyInput3";

    Map<String, String> actual = ArgumentParser.getArguments(input, pattern);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPatternDoNotMatchInput() {
    String pattern = "test command <param1> and <param2> with <param3>";
    String input = "test command input1 and 'input 2' and with \"input 3\" ";

    Map<String, String> actual = ArgumentParser.getArguments(input, pattern);
    Assert.assertEquals(Collections.<String, String>emptyMap(), actual);
  }
}
