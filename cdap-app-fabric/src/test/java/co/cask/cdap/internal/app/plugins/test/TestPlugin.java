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

import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import com.google.common.base.Joiner;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Plugin class for testing instantiation with field injection and macro substitution of primitive types and strings.
 */
@Plugin
@Name("TestPlugin")
public class TestPlugin implements Callable<String> {

  protected Config config;

  @Override
  public String call() throws Exception {
    if (config.nullableLongFlag != null && config.nullableLongFlag % 2 == 0) {
        return config.host + "," + Joiner.on(',').join(config.aBoolean, config.aByte, config.aChar, config.aDouble,
                                                       config.aFloat, config.anInt, config.aLong, config.aShort);
    }
    return null;
  }

  public static final class Config extends PluginConfig {

    @Macro
    private String host;

    @Nullable
    private Long nullableLongFlag;

    @Macro
    private boolean aBoolean;

    @Macro
    private byte aByte;

    @Macro
    private char aChar;

    @Macro
    private double aDouble;

    @Macro
    private float aFloat;

    @Macro
    @Nullable
    private int anInt;

    @Macro
    private long aLong;

    @Macro
    private short aShort;
  }
}
