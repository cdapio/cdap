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

package io.cdap.cdap.internal.provision.adapters;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.Constants.AppMetaStore;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.ApplicationSpecificationCodec;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.store.adapters.AppSpecDeserializationContext;
import io.cdap.cdap.internal.app.store.adapters.AppSpecDeserializationContextHolder;
import io.cdap.cdap.internal.app.store.adapters.PluginClassSerializer;
import io.cdap.cdap.internal.app.store.adapters.PluginDeserializer;
import io.cdap.cdap.internal.provision.ProvisioningTaskInfo;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.store.StoreDefinition;

/**
 * Manages Gson instances specifically configured for {@link ProvisioningTaskInfo} serialization and
 * deserialization, handling application specification reduction. This ensures thread-safe
 * operations by managing the {@link AppSpecDeserializationContext} lifecycle per operation via
 * {@link AppSpecDeserializationContextHolder}. It uses a static cache for Gson instances based on
 * the {@code appSpecReductionEnabled} flag.
 */
public final class ProvisioningTaskInfoAdapter {

  private static final Gson GSON_INSTANCE_REDUCTION_ENABLED = buildGsonInternal(true);
  private static final Gson GSON_INSTANCE_REDUCTION_DISABLED = buildGsonInternal(false);

  private ProvisioningTaskInfoAdapter() {
  }

  private static StructuredTable getPluginDataTable(StructuredTableContext context) {
    try {
      return context.getTable(StoreDefinition.ArtifactStore.PLUGIN_DATA_TABLE);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static StructuredTable getUniversalPluginDataTable(StructuredTableContext context) {
    try {
      return context.getTable(StoreDefinition.ArtifactStore.UNIV_PLUGIN_DATA_TABLE);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean isAppSpecReductionEnabled(StructuredTableContext context) {
    return AppMetaStore.APPSPEC_REDUCTION_SUPPORTED_STORAGE_PROVIDERS.contains(
        context.getStorageProvider());
  }

  /**
   * Deserializes a JSON string to an object of the specified class, managing the
   * {@link AppSpecDeserializationContext} lifecycle via
   * {@link AppSpecDeserializationContextHolder}.
   */
  public static ProvisioningTaskInfo fromJson(String jsonString, StructuredTableContext context) {
    boolean appSpecReductionEnabled = isAppSpecReductionEnabled(context);
    if (appSpecReductionEnabled) {
      AppSpecDeserializationContext operationContext = new AppSpecDeserializationContext(null,
          getPluginDataTable(context), getUniversalPluginDataTable(context));
      AppSpecDeserializationContextHolder.setContext(operationContext);
    }
    try {
      Gson gson = getGson(appSpecReductionEnabled);
      return gson.fromJson(jsonString, ProvisioningTaskInfo.class);
    } finally {
      AppSpecDeserializationContextHolder.clearContext();
    }
  }

  /**
   * Serializes an object to its JSON representation.
   */
  public static String toJson(Object objectToSerialize, StructuredTableContext context) {
    Gson gson = getGson(isAppSpecReductionEnabled(context));
    // No need to set / get the AppSpecDeserialization context in case of serialization.
    return gson.toJson(objectToSerialize, ProvisioningTaskInfo.class);
  }

  private static Gson buildGsonInternal(boolean appSpecReductionEnabled) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(gsonBuilder);
    gsonBuilder.registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec());
    gsonBuilder.registerTypeAdapter(Arguments.class, new ArgumentsCodec());

    if (appSpecReductionEnabled) {
      gsonBuilder.registerTypeAdapter(ProgramDescriptor.class, new ProgramDescriptorDeserializer());
      gsonBuilder.registerTypeAdapter(PluginClass.class, new PluginClassSerializer());
      gsonBuilder.registerTypeAdapter(Plugin.class, new PluginDeserializer());
      gsonBuilder.registerTypeHierarchyAdapter(ApplicationSpecification.class,
          new ApplicationSpecificationCodec());
    }

    return gsonBuilder.create();
  }

  private static Gson getGson(boolean appSpecReductionEnabled) {
    if (appSpecReductionEnabled) {
      return GSON_INSTANCE_REDUCTION_ENABLED;
    }
    return GSON_INSTANCE_REDUCTION_DISABLED;
  }
}
