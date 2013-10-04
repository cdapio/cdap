package com.continuuity.common.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimeMathParserTest {

  @Test
  public void testGetNowInSeconds() {
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    // in case we're on the border between seconds
    long result = TimeMathParser.nowInSeconds();
    Assert.assertTrue((result - now == 1) || result == now);
  }

  @Test
  public void testParseTimestamp() {
    Assert.assertEquals(1234567890, TimeMathParser.parseTime("1234567890"));
    Assert.assertEquals(1234567890, TimeMathParser.parseTime(0, "1234567890"));
  }

  @Test
  public void testParseNow() {
    long now = TimeMathParser.nowInSeconds();
    long result = TimeMathParser.parseTime("NOW");
    // in case we're on the border between seconds
    Assert.assertTrue((result - now) == 1 || (result == now));
    Assert.assertEquals(1234567890, TimeMathParser.parseTime(1234567890, "NOW"));
  }

  @Test
  public void testOneOperation() {
    long now = TimeMathParser.nowInSeconds();
    Assert.assertEquals(now - 7, TimeMathParser.parseTime(now, "NOW-7SECONDS"));
    Assert.assertEquals(now - 7 * 60, TimeMathParser.parseTime(now, "NOW-7MINUTES"));
    Assert.assertEquals(now - 7 * 3600, TimeMathParser.parseTime(now, "NOW-7HOURS"));
    Assert.assertEquals(now - 7 * 86400, TimeMathParser.parseTime(now, "NOW-7DAYS"));
    Assert.assertEquals(now + 7, TimeMathParser.parseTime(now, "NOW+7SECONDS"));
    Assert.assertEquals(now + 7 * 60, TimeMathParser.parseTime(now, "NOW+7MINUTES"));
    Assert.assertEquals(now + 7 * 3600, TimeMathParser.parseTime(now, "NOW+7HOURS"));
    Assert.assertEquals(now + 7 * 86400, TimeMathParser.parseTime(now, "NOW+7DAYS"));
  }

  @Test
  public void testMultipleOperations() {
    long now = TimeMathParser.nowInSeconds();
    Assert.assertEquals(now - 7 * 86400 + 3 * 3600 - 13 * 60 + 11,
                        TimeMathParser.parseTime(now, "NOW-7DAYS+3HOURS-13MINUTES+11SECONDS"));
  }

  // happens if input is supposed to be url encoded but is not
  @Test(expected = IllegalArgumentException.class)
  public void testSpaceInsteadOfPlusThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "NOW 6HOURS");
  }

  // happens if input is supposed to be url encoded but is not
  @Test(expected = IllegalArgumentException.class)
  public void testGibberishInMiddleThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "NOW-3DAYS+23lnkfasd-6HOURS");
  }
}
