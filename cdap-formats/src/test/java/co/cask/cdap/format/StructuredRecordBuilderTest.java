/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.format;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Tests conversion logic
 */
public class StructuredRecordBuilderTest {

  @Test
  public void testDateConversion() {
    long ts = 0L;
    Date date = new Date(ts);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    Schema schema = Schema.recordOf("x1",
                                    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("date1", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("date2", Schema.of(Schema.Type.STRING)));

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("ts", 0L)
      .set("date1", "1970-01-01T00:00:00 UTC")
      .set("date2", "1970-01-01")
      .build();

    StructuredRecord actual = StructuredRecord.builder(schema)
      .convertAndSet("ts", date)
      .convertAndSet("date1", date)
      .convertAndSet("date2", date, dateFormat)
      .build();

    Assert.assertEquals(expected, actual);
  }
}
