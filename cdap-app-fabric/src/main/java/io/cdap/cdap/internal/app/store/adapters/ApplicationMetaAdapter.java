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

package io.cdap.cdap.internal.app.store.adapters;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.ApplicationSpecificationCodec;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.spi.data.StructuredTable;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Gson instances specifically configured for ApplicationMeta serialization and
 * deserialization, handling application specification reduction. This ensures thread-safe
 * operations by managing the {@link AppSpecDeserializationContext} lifecycle per operation via
 * {@link AppSpecDeserializationContextHolder}. It uses a static cache for Gson instances based on
 * the {@code appSpecReductionEnabled} flag.
 */
public final class ApplicationMetaAdapter {

  private static final Gson GSON_INSTANCE_REDUCTION_ENABLED = buildGsonInternal(true);
  private static final Gson GSON_INSTANCE_REDUCTION_DISABLED = buildGsonInternal(false);
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMetaAdapter.class);

  private ApplicationMetaAdapter() {
  }

  /**
   * Deserializes a JSON string to an object of the specified class, managing the
   * {@link AppSpecDeserializationContext} lifecycle via
   * {@link AppSpecDeserializationContextHolder}.
   */
  public static <T> T fromJson(String jsonString, Class<T> classOfT, String namespace,
      boolean appSpecReductionEnabled, StructuredTable pluginDataTable,
      @Nullable StructuredTable universalPluginDataTable) {
    AppSpecDeserializationContext operationContext = new AppSpecDeserializationContext(namespace,
        pluginDataTable, universalPluginDataTable);
    AppSpecDeserializationContextHolder.setContext(operationContext);
    try {
      Gson gson = getGson(appSpecReductionEnabled);
      T result = gson.fromJson(jsonString, classOfT);
      Set<String> missingPlugins = operationContext.getMissingPlugins();
      if (!missingPlugins.isEmpty()) {
        LOG.trace("For application {} : {}", operationContext.getAppName(),
            operationContext.getMissingPlugins());
      }
      return result;
    } finally {
      AppSpecDeserializationContextHolder.clearContext();
    }
  }

  /**
   * Serializes an object to its JSON representation.
   */
  public static String toJson(Object objectToSerialize, boolean appSpecReductionEnabled) {
    Gson gson = getGson(appSpecReductionEnabled);
    // No need to set / get the AppSpecDeserialization context in case of serialization.
    return gson.toJson(objectToSerialize);
  }

  private static Gson buildGsonInternal(boolean appSpecReductionEnabled) {
    GsonBuilder gsonBuilder = new GsonBuilder();

    if (appSpecReductionEnabled) {
      gsonBuilder.registerTypeAdapter(ApplicationMeta.class, new ApplicationMetaCodec());
      gsonBuilder.registerTypeAdapter(PluginClass.class, new PluginClassSerializer());
      gsonBuilder.registerTypeAdapter(Plugin.class, new PluginDeserializer());
      gsonBuilder.registerTypeHierarchyAdapter(ApplicationSpecification.class,
          new ApplicationSpecificationCodec());
    }
    // This adds other necessary adapters for ApplicationSpecification internals (schemas, etc.)
    ApplicationSpecificationAdapter.addTypeAdapters(gsonBuilder);

    return gsonBuilder.create();
  }

  /**
   * Gets a pre-configured, cached, and thread-safe Gson instance.
   *
   * @param conf Configuration map, used to determine if app spec reduction is enabled.
   * @return A shared Gson instance.
   */
  private static Gson getGson(boolean appSpecReductionEnabled) {
    if (appSpecReductionEnabled) {
      return GSON_INSTANCE_REDUCTION_ENABLED;
    }
    return GSON_INSTANCE_REDUCTION_DISABLED;
  }
}
