/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.plugin;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.store.StoreDefinition.ArtifactStore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Util for building plugins for tests.
 */
public final class Plugins {

  private Plugins() {
  }

  /**
   * Creates dummy plugins.
   */
  public static Map<String, Plugin> createDummyPlugins() {
    Map<String, Plugin> plugins = new HashMap<>();

    Gson gson = new Gson();
    Map<String, String> childProperties = ImmutableMap.of("child1", "childVal1", "child2",
        "${secure(acc)}", "child3", "val3");
    Map<String, String> properties = ImmutableMap.of("key2", gson.toJson(childProperties), "key1",
        "val1");

    PluginClass wranglerPluginClass = PluginClass.builder()
        .setClassName("io.cdap.wrangler.Wrangler").setName("Wrangler").setConfigFieldName("config")
        .setRequirements(Requirements.EMPTY).setType("transform")
        .setDescription("Wrangler - A interactive tool for data cleansing and transformation.")
        .add("schema",
            new PluginPropertyField("schema", "Specifies the schema that has to be output.",
                "string", true, true)).add("preconditionSQL",
            new PluginPropertyField("preconditionSQL",
                "SQL Precondition expression specifying filtering before applying directives (false to filter)",
                "string", false, true, false)).build();
    Plugin wranglerPlugin = new Plugin(Collections.emptyList(),
        NamespaceId.DEFAULT.artifact("wrangler-transform", "1.0").toApiArtifactId(),
        wranglerPluginClass, PluginProperties.builder().addAll(properties).build());
    plugins.put("p1", wranglerPlugin);

    PluginClass directivePluginClass = PluginClass.builder()
        .setClassName("com.google.cloud.datafusion.directives.Now").setName("now")
        .setRequirements(Requirements.EMPTY).setType("directive")
        .setDescription("Populates a column with the current date and time").build();
    ArtifactId parent = NamespaceId.DEFAULT.artifact("Wrangler", "1.0").toApiArtifactId();
    Plugin directivePlugin = new Plugin(Collections.singleton(parent),
        NamespaceId.DEFAULT.artifact("wrangler", "1.0").toApiArtifactId(), directivePluginClass,
        PluginProperties.builder().addAll(properties).build());
    plugins.put("p2", directivePlugin);

    PluginClass gCloudFormatTextPluginClass = PluginClass.builder()
        .setClassName("io.cdap.plugin.format.text.input.TextInputFormatProvider").setName("text")
        .setRequirements(Requirements.EMPTY).setType("validatingInputFormat")
        .setDescription("Plugin for reading files in text format.").setConfigFieldName("conf")
        .build();
    parent = NamespaceId.DEFAULT.artifact("google-cloud", "1.0").toApiArtifactId();
    Plugin gCloudFormatTextPlugin = new Plugin(Collections.singleton(parent),
        NamespaceId.DEFAULT.artifact("format-text", "1.0").toApiArtifactId(),
        gCloudFormatTextPluginClass, PluginProperties.builder().addAll(properties).build());
    plugins.put("p3", gCloudFormatTextPlugin);

    return plugins;
  }

  public static Map<String, Plugin> createDummyPluginsForReducedAppSpec() {
    Map<String, Plugin> plugins = new HashMap<>();

    Gson gson = new Gson();
    Map<String, String> childProperties = ImmutableMap.of("child1", "childVal1", "child2",
        "${secure(acc)}", "child3", "val3");
    Map<String, String> properties = ImmutableMap.of("key2", gson.toJson(childProperties), "key1",
        "val1");

    PluginClass wranglerPluginClass = new PluginClass("transform", "Wrangler", null, null,
        Collections.emptyMap(), new Requirements(Collections.emptySet()), "");
    Plugin wranglerPlugin = new Plugin(Collections.emptyList(),
        NamespaceId.DEFAULT.artifact("wrangler-transform", "1.0").toApiArtifactId(),
        wranglerPluginClass, PluginProperties.builder().addAll(properties).build());
    plugins.put("p1", wranglerPlugin);

    PluginClass directivePluginClass = new PluginClass("directive", "now", null, null,
        Collections.emptyMap(), new Requirements(Collections.emptySet()), "");
    ArtifactId parent = NamespaceId.DEFAULT.artifact("Wrangler", "1.0").toApiArtifactId();
    Plugin directivePlugin = new Plugin(Collections.singleton(parent),
        NamespaceId.DEFAULT.artifact("wrangler", "1.0").toApiArtifactId(), directivePluginClass,
        PluginProperties.builder().addAll(properties).build());
    plugins.put("p2", directivePlugin);

    PluginClass gCloudFormatTextPluginClass = PluginClass.builder()
        .setClassName("io.cdap.plugin.format.text.input.TextInputFormatProvider").setName("text")
        .setRequirements(Requirements.EMPTY).setType("validatingInputFormat")
        .setDescription("Plugin for reading files in text format.").setConfigFieldName("conf")
        .build();
    parent = NamespaceId.DEFAULT.artifact("google-cloud", "1.0").toApiArtifactId();
    Plugin gCloudFormatTextPlugin = new Plugin(Collections.singleton(parent),
        NamespaceId.DEFAULT.artifact("format-text", "1.0").toApiArtifactId(),
        gCloudFormatTextPluginClass, PluginProperties.builder().addAll(properties).build());
    plugins.put("p3", gCloudFormatTextPlugin);

    return plugins;
  }

  private static void addPluginEntryToTable(StructuredTable pluginDataTable,
      String parentContextName, String pluginType, String pluginName, String artifactName,
      String pluginDataJson) throws IOException {
    Collection<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(ArtifactStore.PARENT_NAMESPACE_FIELD, "default"));
    fields.add(Fields.stringField(ArtifactStore.PARENT_NAME_FIELD, parentContextName));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_TYPE_FIELD, pluginType));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_NAME_FIELD, pluginName));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAMESPACE_FIELD, "default"));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_NAME_FIELD, artifactName));
    fields.add(Fields.stringField(ArtifactStore.ARTIFACT_VER_FIELD, "1.0"));
    fields.add(Fields.stringField(ArtifactStore.PLUGIN_DATA_FIELD, pluginDataJson));
    pluginDataTable.upsert(fields);
  }

  /**
   * Initializes a {@link StructuredTable} with a dummy entry for a Wrangler plugin.
   */
  public static void addWranglerPluginToTable(StructuredTable pluginDataTable) throws IOException {
    String wranglerPluginJsonData = "{\"pluginClass\":{\"type\":"
        + "\"transform\",\"name\":\"Wrangler\",\"description\":\"Wrangler - A interactive tool "
        + "for data cleansing and transformation.\",\"className\":\"io.cdap.wrangler.Wrangler\","
        + "\"configFieldName\":\"config\",\"properties\":{\"schema\":{\"name\":\"schema\",\"description"
        + "\":\"Specifies the schema that has to be output.\",\"type\":\"string\",\"required\":true,"
        + "\"macroSupported\":true,\"macroEscapingEnabled\":false,\"children\":[]},\"preconditionSQL\""
        + ":{\"name\":\"preconditionSQL\",\"description\":\"SQL Precondition expression specifying"
        + " filtering before applying directives (false to filter)\",\"type\":\"string\",\"required"
        + "\":false,\"macroSupported\":true,\"macroEscapingEnabled\":false,\"children\":[]}},"
        + "\"requirements\":{\"datasetTypes\":[],\""
        + "capabilities\":[]}},\"artifactLocationPath\":\"/cdap/namespaces/system/artifacts/wrangler-transform"
        + "/4.11.0-SNAPSHOT.d26c4ac8-600a-4bf0-8280-0561037036d8.jar\",\"usableBy\":\""
        + "system:cdap-data-pipeline[6.10.0,7.0.0-SNAPSHOT)\"}"; // Example path/usableBy

    addPluginEntryToTable(pluginDataTable, "testArtifact", "transform", "Wrangler",
        "wrangler-transform", wranglerPluginJsonData);
  }

  /**
   * Initializes a {@link StructuredTable} with a dummy entry for a "now" directive plugin.
   */
  public static void addNowDirectivePluginToTable(StructuredTable pluginDataTable)
      throws IOException {

    ArtifactId parent = NamespaceId.DEFAULT.artifact("Wrangler", "1.0").toApiArtifactId();
    String nowDirectiveJsonData = "{\"pluginClass\":{\"type\":\"directive\",\"name\":\"now\","
        + "\"description\":\"Populates a column with the current date and time\","
        + "\"className\":\"com.google.cloud.datafusion.directives.Now\",\"properties\":{},"
        + "\"requirements\":{\"datasetTypes\":[],\"capabilities\":[]}},"
        + "\"artifactLocationPath\":\"/cdap/namespaces/default/artifacts/now-directive/1.0.0-SNAPSHOT.jar\","
        + "\"usableBy\":\"system:wrangler-transform[4.0.0,5.0.0)\"}";

    addPluginEntryToTable(pluginDataTable, parent.getName(), "directive", "now", "wrangler",
        nowDirectiveJsonData);
  }

  /**
   * Initializes a {@link StructuredTable} with a dummy entry for a "format-text" plugin.
   */
  public static void addFormatTextPluginToTable(StructuredTable pluginDataTable)
      throws IOException {

    String formatTextJsonData =
        "{\"pluginClass\":{\"type\":\"validatingInputFormat\",\"name\":\"text\","
            + "\"description\":\"Plugin for reading files in text format.\","
            + "\"className\":\"io.cdap.plugin.format.text.input.TextInputFormatProvider\","
            + "\"configFieldName\":\"conf\",\"requirements\":{\"datasetTypes\":[],"
            + "\"capabilities\":[]}},\"artifactLocationPath\":\"/cdap/namespaces/system/artifacts/format"
            + "-text/2.14.0-SNAPSHOT.4fed59ac-ef3f-4b76-ac29-d5aa8bc38061.jar\","
            + "\"usableBy\":\"system:cdap-data-pipeline[6.8.0-SNAPSHOT,7.0.0-SNAPSHOT)\"}";

    addPluginEntryToTable(pluginDataTable, "testArtifact", "validatingInputFormat", "text",
        "format-text", formatTextJsonData);
  }
}
