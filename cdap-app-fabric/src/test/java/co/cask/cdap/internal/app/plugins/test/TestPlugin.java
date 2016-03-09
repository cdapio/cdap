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

package co.cask.cdap.internal.app.plugins.test;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Plugin class for testing instantiation with field injection.
 */
@Plugin
@Name("TestPlugin")
public class TestPlugin implements Callable<String> {

  protected Config config;

  @Override
  public String call() throws Exception {
    if (config.timeout % 2 == 0) {
      return Class.forName(config.className).getName();
    }
    return null;
  }

  public static final class Config extends PluginConfig {

    @Name("class.name")
    private String className;

    @Nullable
    private Long timeout;
  }
}
