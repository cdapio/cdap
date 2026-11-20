/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.filter;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link FilterParser}.
 */
public class FilterParserTest {

  @Test
  public void testValidExpressions() {
    Assert.assertSame(Filter.EMPTY_FILTER, FilterParser.parse(""));
    Assert.assertSame(Filter.EMPTY_FILTER, FilterParser.parse("''"));
    Assert.assertSame(Filter.EMPTY_FILTER, FilterParser.parse("\"\""));

    Filter single = FilterParser.parse(".namespaceId=default");
    Assert.assertEquals("MdcExpression{key=.namespaceId, value=default}", single.toString());

    Filter compound = FilterParser.parse("MDC:tag=v1 AND .applicationId=app1 OR loglevel=WARN");
    Assert.assertTrue(compound instanceof AndFilter);
  }

  @Test
  public void testUncheckedRuntimeExceptionsAreNotDoubleWrapped() {
    try {
      FilterParser.parse("unknownKey=val");
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertNull(e.getCause());
      Assert.assertTrue(e.getMessage().contains("unknownKey"));
    }

    try {
      FilterParser.parse(".namespaceId");
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertNull(e.getCause());
      Assert.assertTrue(e.getMessage().contains("Expected operator ="));
    }

    try {
      FilterParser.parse(".namespaceId=");
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      Assert.assertNull(e.getCause());
      Assert.assertTrue(e.getMessage().contains("Expected operand"));
    }

    try {
      FilterParser.parse(".namespaceId=default XOR .applicationId=app1");
      Assert.fail("Expected IllegalArgumentException for invalid operator");
    } catch (IllegalArgumentException e) {
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testCheckedIoExceptionPropagatedAsRuntimeException() throws Exception {
    final IOException ioError = new IOException("tokenizer stream failure");
    for (String helperMethod : new String[]{"parseString", "parseEquals", "parseOperator"}) {
      Method method = FilterParser.class.getDeclaredMethod(helperMethod, StreamTokenizer.class);
      method.setAccessible(true);
      StreamTokenizer failingTokenizer = new StreamTokenizer(new Reader() {
        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
          throw ioError;
        }

        @Override
        public void close() {
        }
      });

      try {
        method.invoke(null, failingTokenizer);
        Assert.fail("Expected RuntimeException wrapping IOException from " + helperMethod);
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        Assert.assertEquals(RuntimeException.class, target.getClass());
        Assert.assertSame(ioError, target.getCause());
      }
    }
  }
}
