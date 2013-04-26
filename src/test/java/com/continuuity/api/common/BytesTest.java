package com.continuuity.api.common;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class BytesTest {

  @Test
  public void testIndexOfPositiveTestCases() throws Exception {
    byte [] source = { 10,11,12,13,14,15,16 };
    byte [] target1 = { 13,14};
    assertEquals(3, Bytes.indexOf(source,target1));

    byte [] target2 = { 15,16};
    assertEquals(5, Bytes.indexOf(source,target2));
  }

  @Test
  public void testIndexOfNegativeTestCases() throws Exception {
    byte [] source = { 10,11,12,13,14,15,16 };
    byte [] target1 = { 11,14};
    assertEquals(-1, Bytes.indexOf(source,target1));

    byte [] target2 = {16,17};
    assertEquals(-1, Bytes.indexOf(source,target2));
  }

  @Test(expected = RuntimeException.class)
  public void testSourceNull() throws Exception {
    byte [] target  = {11,14};
    Bytes.indexOf(null, target);
  }

  @Test(expected = RuntimeException.class)
  public void testTargetNull() throws Exception {
    byte [] source  = {11,14};
    Bytes.indexOf(source,null);
  }

  @Test
  public void testZeroLengthByteArray() throws Exception {
    byte [] source  = new byte[0];
    byte [] target  = new byte[0];

    int index = Bytes.indexOf(source,target);
    //The behavior is similar to StringUtils.indexOf("","") - which returns index = 0
    assertEquals(0, index);
  }
}
