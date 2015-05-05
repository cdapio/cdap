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

package co.cask.cdap.data2.dataset2.lib.table;

import com.google.common.base.Objects;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 *
 */
public class Record {
  private final int intField;
  private final Long longField;
  private final Float floatField;
  private final Double doubleField;
  private final String stringField;
  private final byte[] byteArrayField;
  private final ByteBuffer byteBufferField;
  private final UUID uuidField;

  public Record(int intField, Long longField, Float floatField, Double doubleField, String stringField,
                byte[] byteArrayField, ByteBuffer byteBufferField, UUID uuidField) {
    this.intField = intField;
    this.longField = longField;
    this.floatField = floatField;
    this.doubleField = doubleField;
    this.stringField = stringField;
    this.byteArrayField = byteArrayField;
    this.byteBufferField = byteBufferField;
    this.uuidField = uuidField;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Record)) {
      return false;
    }

    Record that = (Record) o;

    return Objects.equal(intField, that.intField) &&
      Objects.equal(longField, that.longField) &&
      Objects.equal(floatField, that.floatField) &&
      Objects.equal(doubleField, that.doubleField) &&
      Objects.equal(stringField, that.stringField) &&
      Arrays.equals(byteArrayField, that.byteArrayField) &&
      Objects.equal(byteBufferField, that.byteBufferField) &&
      Objects.equal(uuidField, that.uuidField);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(intField, longField, floatField, doubleField,
                            stringField, byteArrayField, byteBufferField, uuidField);
  }
}
