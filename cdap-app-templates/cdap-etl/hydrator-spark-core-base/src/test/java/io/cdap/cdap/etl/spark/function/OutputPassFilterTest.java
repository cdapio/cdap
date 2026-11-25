/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.function;

import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link OutputPassFilter}.
 */
public class OutputPassFilterTest {

  @Test
  public void testDefaultNullPortFilter() throws Exception {
    OutputPassFilter<String> filter = new OutputPassFilter<>();

    RecordInfo<Object> nullPortOutput =
        RecordInfo.builder((Object) "v1", "stage1", RecordType.OUTPUT).build();
    Iterator<String> matched = filter.call(nullPortOutput);
    Assert.assertTrue(matched.hasNext());
    Assert.assertEquals("v1", matched.next());
    Assert.assertFalse(matched.hasNext());

    RecordInfo<Object> namedPortOutput =
        RecordInfo.builder((Object) "v2", "stage1", RecordType.OUTPUT).fromPort("portA").build();
    Assert.assertFalse(filter.call(namedPortOutput).hasNext());

    RecordInfo<Object> errorRecord =
        RecordInfo.builder((Object) "err", "stage1", RecordType.ERROR).build();
    Assert.assertFalse(filter.call(errorRecord).hasNext());
  }

  @Test
  public void testNamedPortFilter() throws Exception {
    OutputPassFilter<String> filter = new OutputPassFilter<>("portA");

    RecordInfo<Object> matchingPortOutput =
        RecordInfo.builder((Object) "v1", "stage1", RecordType.OUTPUT).fromPort("portA").build();
    Iterator<String> matched = filter.call(matchingPortOutput);
    Assert.assertTrue(matched.hasNext());
    Assert.assertEquals("v1", matched.next());
    Assert.assertFalse(matched.hasNext());

    RecordInfo<Object> otherPortOutput =
        RecordInfo.builder((Object) "v2", "stage1", RecordType.OUTPUT).fromPort("portB").build();
    Assert.assertFalse(filter.call(otherPortOutput).hasNext());

    RecordInfo<Object> nullPortOutput =
        RecordInfo.builder((Object) "v3", "stage1", RecordType.OUTPUT).build();
    Assert.assertFalse(filter.call(nullPortOutput).hasNext());
  }
}
