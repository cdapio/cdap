/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.spark;

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.streaming.Windower;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Window plugin.
 */
public class Window extends Windower {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private final Conf conf;

  public Window(Conf conf) {
    this.conf = conf;
  }

  @Override
  public long getWidth() {
    return conf.width;
  }

  @Override
  public long getSlideInterval() {
    return conf.slideInterval;
  }

  /**
   * Config for window plugin.
   */
  public static class Conf {
    long width;

    long slideInterval;
  }

  public static ETLPlugin getPlugin(long width, long slideInterval) throws IOException {
    return new ETLPlugin("Window", Windower.PLUGIN_TYPE,
                         ImmutableMap.of("width", String.valueOf(width),
                                         "slideInterval", String.valueOf(slideInterval)),
                         null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("width", new PluginPropertyField("width", "", "long", true, false));
    properties.put("slideInterval", new PluginPropertyField("slideInterval", "", "long", true, false));
    return new PluginClass(Windower.PLUGIN_TYPE, "Window", "", Window.class.getName(), "conf", properties);
  }
}
