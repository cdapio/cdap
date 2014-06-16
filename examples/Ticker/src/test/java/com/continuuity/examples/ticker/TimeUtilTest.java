/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.ticker;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

/**
 *
 */
public class TimeUtilTest {

  @Test
  public void testDateParse() throws ParseException {
    Assert.assertEquals(1382535000, TimeUtil.netfondsDateToTimestamp("20131023T153000"));
  }

  @Test
  public void testNowMath() {
    long now = 1234567890;
    Assert.assertEquals(now - 5, TimeUtil.parseTime(now, "now-5s"));
    Assert.assertEquals(now - 5 * 60, TimeUtil.parseTime(now, "now-5m"));
    Assert.assertEquals(now - 5 * 60 * 60, TimeUtil.parseTime(now, "now-5h"));
    Assert.assertEquals(now - 5 * 24 * 60 * 60, TimeUtil.parseTime(now, "now-5d"));
  }
}
