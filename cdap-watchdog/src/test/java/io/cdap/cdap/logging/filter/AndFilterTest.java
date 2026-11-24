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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AndFilter}.
 */
public class AndFilterTest {

  @Test
  public void testToStringAndMatch() {
    Filter first = new MdcExpression(".namespaceId", "default");
    Filter second = new MdcExpression("MDC:env", "prod");
    AndFilter andFilter = new AndFilter(ImmutableList.of(first, second));

    Assert.assertEquals(
        "AndFilter{expressions=[MdcExpression{key=.namespaceId, value=default}, "
            + "MdcExpression{key=MDC:env, value=prod}]}",
        andFilter.toString());

    LoggingEvent matchingEvent = new LoggingEvent();
    matchingEvent.setMDCPropertyMap(ImmutableMap.of(".namespaceId", "default", "MDC:env", "prod"));
    Assert.assertTrue(andFilter.match(matchingEvent));

    LoggingEvent partialEvent = new LoggingEvent();
    partialEvent.setMDCPropertyMap(ImmutableMap.of(".namespaceId", "default", "MDC:env", "dev"));
    Assert.assertFalse(andFilter.match(partialEvent));
  }
}
