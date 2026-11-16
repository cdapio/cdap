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

import ch.qos.logback.classic.spi.LoggingEvent;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link MdcExpression}.
 */
public class MdcExpressionTest {

  @Test
  public void testToStringAndMatch() {
    MdcExpression expr = new MdcExpression(".namespaceId", "analytics");
    Assert.assertEquals("MdcExpression{key=.namespaceId, value=analytics}", expr.toString());
    Assert.assertEquals(".namespaceId", expr.getKey());
    Assert.assertEquals("analytics", expr.getValue());

    LoggingEvent matchEvent = new LoggingEvent();
    matchEvent.setMDCPropertyMap(Collections.singletonMap(".namespaceId", "analytics"));
    Assert.assertTrue(expr.match(matchEvent));

    LoggingEvent mismatchEvent = new LoggingEvent();
    mismatchEvent.setMDCPropertyMap(Collections.singletonMap(".namespaceId", "other"));
    Assert.assertFalse(expr.match(mismatchEvent));

    LoggingEvent emptyMdcEvent = new LoggingEvent();
    emptyMdcEvent.setMDCPropertyMap(Collections.emptyMap());
    Assert.assertFalse(expr.match(emptyMdcEvent));
  }
}
