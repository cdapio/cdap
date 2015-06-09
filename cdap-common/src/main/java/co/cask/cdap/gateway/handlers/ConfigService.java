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
import co.cask.cdap.proto.ConfigEntry;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.StringWriter;
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
}
