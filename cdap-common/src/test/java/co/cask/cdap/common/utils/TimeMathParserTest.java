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

package co.cask.cdap.common.utils;

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
    long result = TimeMathParser.parseTime("now");
    // in case we're on the border between seconds
    Assert.assertTrue((result - now) == 1 || (result == now));
    Assert.assertEquals(1234567890, TimeMathParser.parseTime(1234567890, "now"));
  }

  @Test
  public void testOneOperation() {
    long now = TimeMathParser.nowInSeconds();
    Assert.assertEquals(now - 7, TimeMathParser.parseTime(now, "now-7s"));
    Assert.assertEquals(now - 7 * 60, TimeMathParser.parseTime(now, "now-7m"));
    Assert.assertEquals(now - 7 * 3600, TimeMathParser.parseTime(now, "now-7h"));
    Assert.assertEquals(now - 7 * 86400, TimeMathParser.parseTime(now, "now-7d"));
    Assert.assertEquals(now + 7, TimeMathParser.parseTime(now, "now+7s"));
    Assert.assertEquals(now + 7 * 60, TimeMathParser.parseTime(now, "now+7m"));
    Assert.assertEquals(now + 7 * 3600, TimeMathParser.parseTime(now, "now+7h"));
    Assert.assertEquals(now + 7 * 86400, TimeMathParser.parseTime(now, "now+7d"));
  }

  @Test
  public void testResolutionParsing() {
    String resolution = "60s";
    Assert.assertEquals(60, TimeMathParser.resolutionInSeconds(resolution));
    resolution = "2m";
    Assert.assertEquals(120, TimeMathParser.resolutionInSeconds(resolution));
    resolution = "3h";
    Assert.assertEquals(10800, TimeMathParser.resolutionInSeconds(resolution));
    resolution = "1d";
    Assert.assertEquals(86400, TimeMathParser.resolutionInSeconds(resolution));
    resolution = "1h3m";
    Assert.assertEquals(3780, TimeMathParser.resolutionInSeconds(resolution));
  }

  @Test
  public void testMultipleOperations() {
    long now = TimeMathParser.nowInSeconds();
    Assert.assertEquals(now - 7 * 86400 + 3 * 3600 - 13 * 60 + 11,
                        TimeMathParser.parseTime(now, "now-7d+3h-13m+11s"));
  }

  // happens if input is supposed to be url encoded but is not
  @Test(expected = IllegalArgumentException.class)
  public void testSpaceInsteadOfPlusThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "now 6h");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGibberishInMiddleThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "now-3d+23lnkfasd-6h");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidOperationThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "now/1d");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnitThrowsException() {
    long now = TimeMathParser.nowInSeconds();
    TimeMathParser.parseTime(now, "now-1w");
  }
}
