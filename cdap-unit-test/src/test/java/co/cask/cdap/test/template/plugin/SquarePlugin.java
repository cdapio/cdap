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

package co.cask.cdap.test.template.plugin;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.templates.plugins.PluginConfig;

import java.util.concurrent.Callable;

/**
 * Template plugin for tests
 */
@Plugin(type = "callable")
@Name("square")
public class SquarePlugin implements Callable<Long> {
  private Config config;

  @Override
  public Long call() throws Exception {
    return config.x * config.x;
  }

  public static final class Config extends PluginConfig {
    @Name("x")
    private Long x;
  }
}
