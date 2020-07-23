/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for Exceptions.
 */
public class ExceptionsTest {
  @Test
  public void testCondenseMultipleExceptions() {
    String m1 = "m1";
    String m2 = "m2";
    String m3 = "m3";
    Exception e1 = new Exception(m1);
    Exception e2 = new Exception(m2, e1);
    Exception e3 = new Exception(m3, e2);
    String expected = m3 + Exceptions.CONDENSE_COMBINER_STRING + m2 + Exceptions.CONDENSE_COMBINER_STRING + m1;
    String condensedMessage = Exceptions.condenseThrowableMessage(e3);
    Assert.assertEquals(expected, condensedMessage);
  }

  @Test
  public void testCondenseExceptionWithNullMessage() {
    String m1 = "m1";
    String m2 = null;
    String m3 = "m3";
    Exception e1 = new Exception(m1);
    Exception e2 = new Exception(m2, e1);
    Exception e3 = new Exception(m3, e2);
    String expected = m3 + Exceptions.CONDENSE_COMBINER_STRING + m1;
    String condensedMessage = Exceptions.condenseThrowableMessage(e3);
    Assert.assertEquals(expected, condensedMessage);
  }

  @Test
  public void testCondenseExceptionWithEmptyMessage() {
    String m1 = "m1";
    String m2 = "";
    String m3 = "m3";
    Exception e1 = new Exception(m1);
    Exception e2 = new Exception(m2, e1);
    Exception e3 = new Exception(m3, e2);
    String expected = m3 + Exceptions.CONDENSE_COMBINER_STRING + m1;
    String condensedMessage = Exceptions.condenseThrowableMessage(e3);
    Assert.assertEquals(expected, condensedMessage);
  }
}
