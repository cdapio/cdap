/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.format.utils;

import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Utility class to handle decimal types
 */
public class FormatUtils {

  /**
   * Extract decimal value from a string, using the supplied schema to get precision and scale,
   *
   * @param decimalSchema schema to use
   * @param strVal value to extract
   * @return Bigdecimal value
   * @throws UnexpectedFormatException if the supplied string is not a valid Decimal value for
   *     the supplied schema
   */
  public static BigDecimal parseDecimal(Schema decimalSchema, String strVal) {
    try {
      // Parse BigDecimal using schema precision and scale. Rounding should not be required,
      int precision = decimalSchema.getPrecision();
      int scale = decimalSchema.getScale();
      MathContext mc = new MathContext(precision, RoundingMode.UNNECESSARY);
      return new BigDecimal(strVal, mc).setScale(scale, RoundingMode.UNNECESSARY);
    } catch (NumberFormatException | ArithmeticException e) {
      throw new UnexpectedFormatException("Cannot convert String " + strVal + " to a Decimal"
          + " with precision " + decimalSchema.getPrecision()
          + " and scale " + decimalSchema.getScale(),
          e);
    }
  }

  /**
   * Encode a ByteBuffer or byte array object into a Base64 String
   *
   * @param value value to encode
   * @return Base64 encoded string
   * @throws IOException if the supplied object is not a ByteBuffer or byte array
   */
  public static String base64Encode(Object value) throws IOException {
    if (value instanceof ByteBuffer) {
      return Base64.getEncoder().encodeToString(extractFromByteBuffer((ByteBuffer) value));
    } else if (value.getClass().isArray() && value.getClass().getComponentType()
        .equals(byte.class)) {
      return Base64.getEncoder().encodeToString(((byte[]) value));
    } else {
      throw new IOException("Expected either ByteBuffer or byte[]. Got " + value.getClass());
    }
  }

  /**
   * Decode a Base64 string into a byte buffer
   *
   * @param encoded encoded string
   * @return byte array containing the decoded string;
   */
  public static ByteBuffer base64Decode(String encoded) throws IOException {
    return ByteBuffer.wrap(Base64.getDecoder().decode(encoded));
  }

  /**
   * Extract a byte array from a byte buffer
   *
   * @param buffer byte buffer
   * @return byte[] representing the remaining contents of a byte buffer.
   */
  protected static byte[] extractFromByteBuffer(ByteBuffer buffer) {
    byte[] arr = new byte[buffer.remaining()];
    buffer.get(arr);
    return arr;
  }
}
