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
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link OrFilter}.
 */
public class OrFilterTest {

  @Test
  public void testToStringAndMatch() {
    Filter first = new MdcExpression("MDC:k1", "v1");
    Filter second = new MdcExpression("MDC:k2", "v2");
    OrFilter orFilter = new OrFilter(ImmutableList.of(first, second));

    Assert.assertEquals(
        "OrFilter{expressions=[MdcExpression{key=MDC:k1, value=v1}, "
            + "MdcExpression{key=MDC:k2, value=v2}]}",
        orFilter.toString());

    LoggingEvent secondMatches = new LoggingEvent();
    secondMatches.setMDCPropertyMap(Collections.singletonMap("MDC:k2", "v2"));
    Assert.assertTrue(orFilter.match(secondMatches));

    LoggingEvent noneMatches = new LoggingEvent();
    noneMatches.setMDCPropertyMap(Collections.singletonMap("MDC:k1", "wrong"));
    Assert.assertFalse(orFilter.match(noneMatches));
  }
}
