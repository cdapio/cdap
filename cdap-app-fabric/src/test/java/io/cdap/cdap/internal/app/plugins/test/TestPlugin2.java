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

package io.cdap.cdap.internal.app.plugins.test;

import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;

/**
 * Plugin class for testing instantiation with constructor.
 */
@Plugin
@Name("TestPlugin2")
@Metadata(
  tags = {"test-tag1", "test-tag2", "test-tag3"},
  properties = {@MetadataProperty(key = "key1", value = "val1"), @MetadataProperty(key = "key2", value = "val2")})
public class TestPlugin2 extends TestPlugin {

  private boolean constructed;

  public TestPlugin2(Config config) {
    this.config = config;
    this.constructed = true;
  }

  @Override
  public String call() throws Exception {
    if (!constructed) {
      throw new IllegalStateException("Constructor not called");
    }
    return super.call();
  }
}
