package com.continuuity.api.common;

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

}
