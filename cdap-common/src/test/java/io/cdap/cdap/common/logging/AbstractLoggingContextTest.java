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

package io.cdap.cdap.common.logging;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractLoggingContext}.
 */
public class AbstractLoggingContextTest {

  private static final class SampleLoggingContext extends AbstractLoggingContext {

    SampleLoggingContext(String tagName, String tagValue) {
      setSystemTag(tagName, tagValue);
    }

    String readTag(String tagName) {
      return getSystemTag(tagName);
    }

    @Override
    public String getLogPartition() {
      return "sample-partition";
    }
  }

  @Test
  public void testSystemTagsAndExactToStringFormats() {
    SampleLoggingContext context = new SampleLoggingContext(".ns", "default");
    Assert.assertEquals("default", context.readTag(".ns"));
    Assert.assertEquals("sample-partition", context.getLogPartition());
    Assert.assertEquals(Collections.singletonMap(".ns", "default"),
        context.getSystemTagsAsString());

    LoggingContext.SystemTag tag = context.getSystemTagsMap().get(".ns");
    Assert.assertNotNull(tag);
    Assert.assertEquals(".ns", tag.getName());
    Assert.assertEquals("default", tag.getValue());
    Assert.assertEquals("SystemTagImpl{name=.ns, value=default}", tag.toString());

    Assert.assertEquals(
        "SampleLoggingContext{systemTags={.ns=SystemTagImpl{name=.ns, value=default}}}",
        context.toString());
  }
}
