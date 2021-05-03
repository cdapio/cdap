/*
 * Copyright © 2015-2021 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Plugin class for testing instantiation with field injection and macro substitution of primitive types and strings.
 */
@Plugin
@Name("TestPlugin")
@Metadata(
  tags = {"tag1", "tag2", "tag3"},
  properties = {@MetadataProperty(key = "k1", value = "v1"), @MetadataProperty(key = "k2", value = "v2")})
public class TestPlugin implements Callable<String> {

  protected Config config;

  @Override
  public String call() throws Exception {
    if (config.nullableLongFlag != null && config.nullableLongFlag % 2 == 0) {
        return config.host + "," + Joiner.on(',').join(config.aBoolean, config.aByte, config.aChar, config.aDouble,
                                                       config.aFloat, config.anInt, config.aLong, config.aShort,
                                                       config.authInfo == null ? "null" : config.authInfo);
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

    @Macro
    @Nullable
    private AuthInfo authInfo;
  }

  public static final class AuthInfo {
    private String token;
    private String id;

    public AuthInfo(String token, String id) {
      this.token = token;
      this.id = id;
    }

    @Override
    public String toString() {
      return "AuthInfo{" +
        "token='" + token + '\'' +
        ", id='" + id + '\'' +
        '}';
    }
  }
}
