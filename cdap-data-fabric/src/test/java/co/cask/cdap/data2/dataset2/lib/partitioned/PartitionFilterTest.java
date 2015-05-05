/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.partitioned;

import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.Partitioning;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for partition filters.
 */
public class PartitionFilterTest {

  @Test(expected = IllegalStateException.class)
  public void testBuilderEmpty() {
    PartitionFilter.builder().build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullField() {
    PartitionFilter.builder().addValueCondition(null, 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullField2() {
    PartitionFilter.builder().addRangeCondition(null, 1, 2).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderEmptyField() {
    PartitionFilter.builder().addValueCondition("", 1).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderEmptyField2() {
    PartitionFilter.builder().addRangeCondition("", 1, 2).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderNullValue() {
    PartitionFilter.builder().<Long>addValueCondition("x", null).build();
  }

  @Test
  public void testBuilderNullRange() {
    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition("a", 1)
      .<Long>addRangeCondition("x", null, null)
      .build();
    Assert.assertEquals(1, filter.getConditions().size()); // only the one for "a"
    Assert.assertNull(filter.getCondition("x"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderDuplicateField() {
    PartitionFilter.builder()
      .addValueCondition("x", 10)
      .addValueCondition("y", 10L)
      .addRangeCondition("x", 14, 15).build();
  }

  @Test
  public void testBuilderGetterMatch() {

    long minute = TimeUnit.MINUTES.toMillis(1);

    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition("year", 2012)
      .addRangeCondition("month", 4, 7)
      .addValueCondition("market", "asia")
      .addRangeCondition("duration", 60 * minute, 90 * minute)
      .build();

    Assert.assertEquals(4, filter.getConditions().size());
    validateCondition(filter, "year", true, Partitioning.FieldType.INT, 2012, null, 2011, 2013);
    validateCondition(filter, "month", false, Partitioning.FieldType.INT, 4, 5, 6, null, 3, 7, 8);
    validateCondition(filter, "market", true, Partitioning.FieldType.STRING, "asia", null, "america", "", "europe");
    validateCondition(filter, "duration", false, Partitioning.FieldType.LONG,
                      60 * minute, 80 * minute, 89 * minute, 90 * minute - 1,
                      null,
                      minute, 30 * minute, 60 * minute - 1, 90 * minute, Long.MAX_VALUE, Long.MIN_VALUE, 0L);

    // should match
    Assert.assertTrue(filter.match(PartitionKey
                                     .builder()
                                     .addField("month", 4)
                                     .addField("duration", 75 * minute)
                                     .addField("market", "asia")
                                     .addField("year", 2012)
                                     .build()));

    // out of range
    Assert.assertFalse(filter.match(PartitionKey
                                      .builder()
                                      .addField("month", 7)
                                      .addField("duration", 75 * minute)
                                      .addField("year", 2012)
                                      .build()));

    // field missing
    Assert.assertFalse(filter.match(PartitionKey
                                      .builder()
                                      .addField("month", 4)
                                      .addField("duration", 75 * minute)
                                      .addField("year", 2012)
                                      .build()));

    // extra field
    Assert.assertTrue(filter.match(PartitionKey
                                     .builder()
                                     .addField("day", "tue")
                                     .addField("month", 4)
                                     .addField("duration", 75 * minute)
                                     .addField("year", 2012)
                                     .addField("market", "asia")
                                     .build()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIncompatibleMatch() {

    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition("year", 2012)
      .addRangeCondition("month", 4, 7)
      .addValueCondition("market", "asia")
      .build();

    // field of incompatible type
    filter.match(PartitionKey
                   .builder()
                   .addField("month", "january")
                   .addField("market", "latin")
                   .addField("year", 2012)
                   .build());
  }

  private <T extends Comparable<T>> void
  validateCondition(PartitionFilter filter, String field, boolean single, Partitioning.FieldType type, T ... values) {
    PartitionFilter.Condition<? extends Comparable> condition = filter.getCondition(field);
    Assert.assertNotNull(condition);
    Assert.assertEquals(single, condition.isSingleValue());
    Assert.assertTrue(FieldTypes.validateType(condition.getValue(), type));
    boolean expectMatch = true;
    for (T value : values) {
      if (value == null) {
        expectMatch = false;
        continue;
      }
      Assert.assertEquals(expectMatch, condition.match(value));
    }
  }

}
