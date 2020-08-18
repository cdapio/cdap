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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Unit test for {@link RuntimeJobTwillPreparer}.
 */
public class RuntimeJobTwillPreparerTest {

  @Test
  public void testParseJvmProperties() {
    Map<String, String> props = RuntimeJobTwillPreparer.parseJvmProperties("-Dkey1=value1 -Dkey2=\"value with space\"");

    Assert.assertEquals("value1", props.get("key1"));
    Assert.assertEquals("value with space", props.get("key2"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingQuote() {
    RuntimeJobTwillPreparer.parseJvmProperties("-Dkey=\"value with space");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingEqual() {
    RuntimeJobTwillPreparer.parseJvmProperties("-Dkey");
  }
}
