package co.cask.cdap.templates;

import co.cask.cdap.api.templates.AdapterPluginRegistry;
import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.api.templates.plugins.PluginProperties;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.api.templates.plugins.PluginSelector;
import co.cask.cdap.internal.app.runtime.adapter.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.adapter.PluginRepository;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class DefaultPluginRegistry implements AdapterPluginRegistry {

  private final Map<String, AdapterPlugin> adapterPlugins = Maps.newHashMap();
  private final PluginRepository pluginRepository;
  private final PluginInstantiator pluginInstantiator;
  private final Id.Program programId;

  public DefaultPluginRegistry(PluginRepository pluginRepository, PluginInstantiator pluginInstantiator,
                               Id.Program programId) {
    this.pluginRepository = pluginRepository;
    this.pluginInstantiator = pluginInstantiator;
    this.programId = programId;
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePlugin(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties, PluginSelector selector) {
    Preconditions.checkArgument(!adapterPlugins.containsKey(pluginId),
                                "Plugin of type {}, name {} was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(programId.toString(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }
    try {
      T instance = pluginInstantiator.newInstance(pluginEntry.getKey(), pluginEntry.getValue(), properties);
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return instance;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties) {
    return usePluginClass(pluginType, pluginName, pluginId, properties, new PluginSelector());
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId, PluginProperties properties, PluginSelector selector) {
    Preconditions.checkArgument(!adapterPlugins.containsKey(pluginId),
                                "Plugin of type %s, name %s was already added.", pluginType, pluginName);
    Preconditions.checkArgument(properties != null, "Plugin properties cannot be null");

    Map.Entry<PluginInfo, PluginClass> pluginEntry = pluginRepository.findPlugin(programId.toString(),
                                                                                 pluginType, pluginName, selector);
    if (pluginEntry == null) {
      return null;
    }

    // Just verify if all required properties are provided.
    // No type checking is done for now.
    for (PluginPropertyField field : pluginEntry.getValue().getProperties().values()) {
      Preconditions.checkArgument(!field.isRequired() || properties.getProperties().containsKey(field.getName()),
                                  "Required property '%s' missing for plugin of type %s, name %s.",
                                  field.getName(), pluginType, pluginName);
    }

    try {
      Class<T> cls = pluginInstantiator.loadClass(pluginEntry.getKey(), pluginEntry.getValue());
      registerPlugin(pluginId, pluginEntry.getKey(), pluginEntry.getValue(), properties);
      return cls;
    } catch (IOException e) {
      // If the plugin jar is deleted without notifying the adapter service.
      return null;
    } catch (ClassNotFoundException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  /**
   * Register the given plugin in this configurer.
   */
  private void registerPlugin(String pluginId, PluginInfo pluginInfo, PluginClass pluginClass,
                              PluginProperties properties) {
    adapterPlugins.put(pluginId, new AdapterPlugin(pluginInfo, pluginClass, properties));
  }
}
