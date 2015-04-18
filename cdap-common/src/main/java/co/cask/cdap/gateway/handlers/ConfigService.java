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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;

/**
 * Exposes {@link CConfiguration} and {@link Configuration}.
 */
public class ConfigService {

  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public ConfigService(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  public List<ConfigEntry> getCConf() {
    return toConfigEntries(cConf);
  }

  public String getCConfXMLString() throws IOException {
    StringWriter stringWriter = new StringWriter();
    cConf.writeXml(stringWriter);
    return stringWriter.toString();
  }

  public List<ConfigEntry> getHConf() {
    return toConfigEntries(hConf);
  }

  public String getHConfXMLString() throws IOException {
    StringWriter stringWriter = new StringWriter();
    hConf.writeXml(stringWriter);
    return stringWriter.toString();
  }

  private String getFirstElement(String[] array) {
    if (array != null && array.length >= 1) {
      return array[0];
    } else {
      return null;
    }
  }

  private List<ConfigEntry> toConfigEntries(Configuration configuration) {
    List<ConfigEntry> result = Lists.newArrayList();
    for (Map.Entry<String, String> entry : configuration) {
      String source = getFirstElement(configuration.getPropertySources(entry.getKey()));
      result.add(new ConfigEntry(entry.getKey(), entry.getValue(), source));
    }
    return result;
  }

  private List<ConfigEntry> toConfigEntries(CConfiguration configuration) {
    List<ConfigEntry> result = Lists.newArrayList();
    for (Map.Entry<String, String> entry : configuration) {
      String source = getFirstElement(configuration.getPropertySources(entry.getKey()));
      result.add(new ConfigEntry(entry.getKey(), entry.getValue(), source));
    }
    return result;
  }

  /**
   * Represents an entry in {@link Configuration} or {@link CConfiguration}.
   */
  public static final class ConfigEntry {
    private final String name;
    private final String value;
    private final String source;

    public ConfigEntry(String name, String value, String source) {
      this.name = name;
      this.value = value;
      this.source = source;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    public String getSource() {
      return source;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, value, source);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final ConfigEntry other = (ConfigEntry) obj;
      return Objects.equal(this.name, other.name) &&
        Objects.equal(this.value, other.value) &&
        Objects.equal(this.source, other.source);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("name", name).add("value", value).add("source", source).toString();
    }
  }
}
