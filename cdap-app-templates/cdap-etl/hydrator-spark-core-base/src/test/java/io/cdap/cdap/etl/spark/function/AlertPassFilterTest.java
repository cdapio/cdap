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

import io.cdap.cdap.etl.api.Alert;
import io.cdap.cdap.etl.common.RecordInfo;
import io.cdap.cdap.etl.common.RecordType;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AlertPassFilter}.
 */
public class AlertPassFilterTest {

  @Test
  public void testCallFiltersAlertsOnly() throws Exception {
    AlertPassFilter filter = new AlertPassFilter();
    Alert alert = new Alert("stage1", Collections.singletonMap("key", "value"));

    RecordInfo<Object> alertRecord =
        RecordInfo.builder((Object) alert, "stage1", RecordType.ALERT).build();
    Iterator<Alert> matched = filter.call(alertRecord);
    Assert.assertTrue(matched.hasNext());
    Assert.assertEquals(alert, matched.next());
    Assert.assertFalse(matched.hasNext());

    RecordInfo<Object> outputRecord =
        RecordInfo.builder((Object) "payload", "stage1", RecordType.OUTPUT).build();
    Assert.assertFalse(filter.call(outputRecord).hasNext());

    RecordInfo<Object> errorRecord =
        RecordInfo.builder((Object) "err", "stage1", RecordType.ERROR).build();
    Assert.assertFalse(filter.call(errorRecord).hasNext());
  }
}
