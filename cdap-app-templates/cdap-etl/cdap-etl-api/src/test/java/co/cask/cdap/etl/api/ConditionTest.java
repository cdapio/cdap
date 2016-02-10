/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

/**
 *
 */
public class ConditionTest {

  @Test
  public void testReplaceVariables() {
    Calendar calendar = new Calendar.Builder()
      .setDate(2013, 6, 10)
      .setTimeOfDay(1, 2, 3)
      .build();

    String result = Condition.replaceVariables(
      calendar, "${start.year} ${start.month} ${start.day} ${start.hour} ${start.minute} ${start.second}");
    Assert.assertEquals("2013 6 10 1 2 3", result);
  }

  @Test
  public void testParsePartitionString() {
    Calendar calendar = new Calendar.Builder()
      .setDate(2013, 6, 10)
      .setTimeOfDay(1, 2, 3)
      .build();

    PartitionKey key = Condition.parsePartitionString(
      calendar, "{\"year\":\"${start.year}\",\"month\":\"${start.month}\"," +
        "\"day\":\"${start.day}\"," +
        "\"hour\":\"${start.hour}\",\"minute\":\"${start.minute}\"}");
    Assert.assertEquals(
      ImmutableMap.builder()
        .put("year", "2013")
        .put("month", "6")
        .put("day", "10")
        .put("hour", "1")
        .put("minute", "2")
        .build(),
      key.getFields()
    );
  }

}
