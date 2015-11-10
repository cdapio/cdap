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

package co.cask.cdap.dq;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Test for Test for {@link FieldDetail}.
 */
public class FieldDetailTest {

  @Test
  public void test() {
    Set<AggregationTypeValue> aggregationTypeValuesSet1 =
      Sets.newHashSet(new AggregationTypeValue("a", true), new AggregationTypeValue("b", true));
    Set<AggregationTypeValue> aggregationTypeValuesSet2 =
      Sets.newHashSet(new AggregationTypeValue("a", true), new AggregationTypeValue("c", true));
    Set<AggregationTypeValue> aggregationTypeValuesSetExpected =
      Sets.newHashSet(new AggregationTypeValue("a", true), new AggregationTypeValue("b", true),
                      new AggregationTypeValue("c", true));
    FieldDetail fieldDetail1 = new FieldDetail("name", aggregationTypeValuesSet1);
    FieldDetail fieldDetail2 = new FieldDetail("name", aggregationTypeValuesSet2);
    fieldDetail1.addAggregations(fieldDetail2.getAggregationTypeSet());
    FieldDetail expected = new FieldDetail("name", aggregationTypeValuesSetExpected);
    Assert.assertEquals(expected, fieldDetail1);
  }
}
