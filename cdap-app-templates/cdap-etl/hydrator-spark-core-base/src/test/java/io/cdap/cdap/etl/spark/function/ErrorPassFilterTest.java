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

import io.cdap.cdap.etl.api.ErrorRecord;
import io.cdap.cdap.etl.common.BasicErrorRecord;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ErrorPassFilter}.
 */
public class ErrorPassFilterTest {

  @Test
  public void testCallFiltersErrorsOnly() throws Exception {
    ErrorPassFilter<String> filter = new ErrorPassFilter<>();
    ErrorRecord<String> error = new BasicErrorRecord<>("bad-row", "stage1", 400, "invalid");

    RecordInfo<Object> errorRecord =
        RecordInfo.builder((Object) error, "stage1", RecordType.ERROR).build();
    Iterator<ErrorRecord<String>> matched = filter.call(errorRecord);
    Assert.assertTrue(matched.hasNext());
    Assert.assertSame(error, matched.next());
    Assert.assertFalse(matched.hasNext());

    RecordInfo<Object> outputRecord =
        RecordInfo.builder((Object) "good-row", "stage1", RecordType.OUTPUT).build();
    Assert.assertFalse(filter.call(outputRecord).hasNext());

    RecordInfo<Object> alertRecord =
        RecordInfo.builder((Object) "alert", "stage1", RecordType.ALERT).build();
    Assert.assertFalse(filter.call(alertRecord).hasNext());
  }
}
