package io.cdap.cdap.proto;

import java.util.Collections;
import java.util.List;

/**
 * The mapping between triggering pipeline properties to the triggered pipeline arguments.
 */
public class TriggeringPropertyMapping {
  private final List<ArgumentMapping> arguments;
  private final List<PluginPropertyMapping> pluginProperties;

  public TriggeringPropertyMapping() {
    this.arguments = Collections.emptyList();
    this.pluginProperties = Collections.emptyList();
  }

  public TriggeringPropertyMapping(List<ArgumentMapping> arguments, List<PluginPropertyMapping> pluginProperties) {
    this.arguments = arguments;
    this.pluginProperties = pluginProperties;
  }

  /**
   * @return The list of mapping between triggering pipeline arguments to triggered pipeline arguments
   */
  public List<ArgumentMapping> getArguments() {
    return arguments;
  }

  /**
   * @return The list of mapping between triggering pipeline plugin properties to triggered pipeline arguments
   */
  public List<PluginPropertyMapping> getPluginProperties() {
    return pluginProperties;
  }

  @Override
  public String toString() {
    return "TriggeringPropertyMapping{" +
      "arguments=" + getArguments() +
      ", pluginProperties=" + getPluginProperties() +
      '}';
  }
}
