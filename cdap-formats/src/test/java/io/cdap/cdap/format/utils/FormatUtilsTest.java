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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class FormatUtilsTest {

  @Test
  public void testParseDecimal1() {
    Schema schema1 = Schema.decimalOf(8, 3);
    BigDecimal bd1 = FormatUtils.parseDecimal(schema1, "12345.678");
    Assert.assertEquals(new BigDecimal(new BigInteger("12345678"), 3), bd1);

    BigDecimal bd2 = FormatUtils.parseDecimal(schema1, "98765.432");
    Assert.assertEquals(new BigDecimal(new BigInteger("98765432"), 3), bd2);

    BigDecimal bd3 = FormatUtils.parseDecimal(schema1, "81");
    Assert.assertEquals(new BigDecimal(new BigInteger("81000"), 3), bd3);
  }

  @Test
  public void testParseDecimal2() {
    Schema schema1 = Schema.decimalOf(21, 9);
    BigDecimal bd1 = FormatUtils.parseDecimal(schema1, "123456789123.456789123");
    Assert.assertEquals(new BigDecimal(new BigInteger("123456789123456789123"), 9), bd1);
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testParseDecimalException1() {
    Schema schema = Schema.decimalOf(8, 3);
    BigDecimal bd = FormatUtils.parseDecimal(schema, "12345.6789");
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testParseDecimalException2() {
    Schema schema1 = Schema.decimalOf(21, 9);
    BigDecimal bd1 = FormatUtils.parseDecimal(schema1, "123456789123.4567891231");
  }

  @Test
  public void testBase64Encode1() throws IOException {
    String text = "This is a test string.";
    String encoded = FormatUtils.base64Encode(text.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("VGhpcyBpcyBhIHRlc3Qgc3RyaW5nLg==", encoded);
  }

  @Test
  public void testBase64Encode2() throws IOException {
    String text = "This is another test string.";
    String encoded = FormatUtils.base64Encode(ByteBuffer.wrap(text.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals("VGhpcyBpcyBhbm90aGVyIHRlc3Qgc3RyaW5nLg==", encoded);
  }

  @Test(expected = IOException.class)
  public void testBase64EncodeException() throws IOException {
    String text = "This is yet another test string.";
    String encoded = FormatUtils.base64Encode(text);
  }

  @Test
  public void testBase64Decode() throws IOException {
    String encoded = "VGhpcyBpcyBhbiBlbmNvZGVkIHV0Zi04IHN0cmluZw==";
    ByteBuffer decoded = FormatUtils.base64Decode(encoded);
    byte[] expected = "This is an encoded utf-8 string".getBytes(StandardCharsets.UTF_8);
    Assert.assertArrayEquals(expected, decoded.array());
  }
}
