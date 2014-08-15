/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Test RuntimeArguments class.
 */
public class RuntimeArgumentsTest {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeArgumentsTest.class);

  @Test
  public void testMaptoString() {
    Map<String, String> testMap = Maps.newHashMap();
    testMap.put("key1", "val1");
    testMap.put("key2", "val2");
    String [] expectedArray = new String[]{"--key1=val1", "--key2=val2"};
    Arrays.sort(expectedArray);
    String [] receivedArray = RuntimeArguments.toPosixArray(testMap);
    Arrays.sort(receivedArray);
    Assert.assertArrayEquals(receivedArray, expectedArray);
    receivedArray = RuntimeArguments.toPosixArray(new BasicArguments(testMap));
    Arrays.sort(receivedArray);
    Assert.assertArrayEquals(receivedArray, expectedArray);
  }

  @Test
  public void testStringtoMap() {
    String[] argArray = new String[] { "--key1=val1" , "--key2=val2"};
    Map<String, String> expectedMap = Maps.newHashMap();
    expectedMap.put("key1", "val1");
    expectedMap.put("key2", "val2");
    Assert.assertTrue(expectedMap.equals(RuntimeArguments.fromPosixArray(argArray)));
  }
}
