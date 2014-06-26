package com.continuuity;

import com.continuuity.api.common.RuntimeArguments;
import com.continuuity.internal.app.runtime.BasicArguments;
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
