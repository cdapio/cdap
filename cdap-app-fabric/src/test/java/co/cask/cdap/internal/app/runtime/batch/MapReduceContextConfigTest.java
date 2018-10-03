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

package co.cask.cdap.internal.app.runtime.batch;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.plugin.Plugin;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.schedule.ScheduleCreationSpec;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;



/**
 */
public class MapReduceContextConfigTest {

    private static final Type PLUGIN_MAP_TYPE = new TypeToken<Map<String, Plugin>>() { }.getType();
    private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
            .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
            .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();
    private static final String HCONF_ATTR_PLUGINS = "cdap.mapreduce.plugins";

    @Test
    public void testManyMacrosInAppSpec() {
        Configuration hConf = new Configuration();
        MapReduceContextConfig cfg = new MapReduceContextConfig(hConf);
        StringBuilder appCfg = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            appCfg.append("${").append(i).append("}");
            hConf.setInt(String.valueOf(i), i);
        }
        ApplicationSpecification appSpec = 
           new DefaultApplicationSpecification("name", "desc", appCfg.toString(),
           new ArtifactId("artifact", 
           new ArtifactVersion("1.0.0"), ArtifactScope.USER),
           Collections.<String, StreamSpecification>emptyMap(), 
           Collections.<String, String>emptyMap(),
           Collections.<String, DatasetCreationSpec>emptyMap(), 
           Collections.<String, FlowSpecification>emptyMap(),
           Collections.<String, MapReduceSpecification>emptyMap(),
           Collections.<String, SparkSpecification>emptyMap(),
           Collections.<String, WorkflowSpecification>emptyMap(),
           Collections.<String, ServiceSpecification>emptyMap(),
           Collections.<String, ScheduleSpecification>emptyMap(),
           Collections.<String, ScheduleCreationSpec>emptyMap(),
           Collections.<String, WorkerSpecification>emptyMap(), 
           Collections.<String, Plugin>emptyMap());
           cfg.setApplicationSpecification(appSpec);
           Assert.assertEquals(appSpec.getConfiguration(), 
           cfg.getApplicationSpecification().getConfiguration());
     }

@Test
public void testGetPluginsWithMacrosMoreThan20() {
  Configuration hConf = new Configuration();
  MapReduceContextConfig cfg = new MapReduceContextConfig(hConf);
  Map<String, Plugin> mockPlugins = new HashMap<String, Plugin>();
  ArtifactId artifactId = new ArtifactId("plugins", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM);
  Map<String, String> properties = new HashMap<String, String>();
  properties.put("path",
      "${input.directory}/${a}${b}${c}${d}${e}${f}${g}${h}${i}${j}"
       + "${k}${l}${m}${n}${o}${p}${q}${r}${s}${t}${u}${v}${w}${x}${y}${z}.txt");
  hConf.set("input.directory", "/dummy/path");
  String[] alphabetsArr = { "a", "b", "c", "d", "e", "f", "g", "h",
      "i", "j", "k", "l", "m", "n", "o", "p", "q",
      "r", "s", "t", "u", "v", "w", "x", "y", "z" };
  for (String alphabet : alphabetsArr) {
     hConf.set(alphabet, alphabet);
  }
  LinkedHashSet<ArtifactId> parents = new LinkedHashSet<>();
  Plugin filePlugin1 = new Plugin(parents, artifactId,
     new PluginClass("type", "name", "desc", "clsname", "cfgfield",
  ImmutableMap.<String, PluginPropertyField>of()),
     PluginProperties.builder().addAll(properties).build());

  mockPlugins.put("File1", filePlugin1);
  hConf.set(HCONF_ATTR_PLUGINS, GSON.toJson(mockPlugins, PLUGIN_MAP_TYPE));

  Map<String, Plugin> plugins = cfg.getPlugins();
  Assert.assertEquals(plugins.size(), 1);
 }
}
