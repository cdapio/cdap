/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.app.runtime.artifact.plugin.nested;

import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Plugin which contains nested plugin config
 */
@Plugin(type = "dummy")
@Name("nested")
@Description("Nested config")
public class NestedConfigPlugin implements Callable<String> {
  private Config config;

  @Override
  public String call() throws Exception {
    return new Gson().toJson(config);
  }

  public static class Config extends PluginConfig {
    @Name("X")
    public int x;

    @Name("Nested")
    @Macro
    public NestedConfig nested;

    public Config(int x, NestedConfig nested) {
      this.x = x;
      this.nested = nested;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Config config = (Config) o;
      return x == config.x &&
               Objects.equals(nested, config.nested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(x, nested);
    }
  }

  public static class NestedConfig extends PluginConfig {
    @Name("Nested1")
    @Macro
    public String nested1;

    @Name("Nested2")
    @Macro
    public String nested2;

    public NestedConfig(String nested1, String nested2) {
      this.nested1 = nested1;
      this.nested2 = nested2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      NestedConfig that = (NestedConfig) o;
      return Objects.equals(nested1, that.nested1) &&
               Objects.equals(nested2, that.nested2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nested1, nested2);
    }
  }
}
