package io.cdap.cdap.spi.events.trigger;

import java.util.List;

public class TriggeringPropertyMapping {
  private final List<ArgumentMapping> argumentMappings;
  private final List<PluginPropertyMapping> pluginPropertyMappings;

  public TriggeringPropertyMapping(List<ArgumentMapping> argumentMappings,
                                   List<PluginPropertyMapping> pluginPropertyMappings) {
    this.argumentMappings = argumentMappings;
    this.pluginPropertyMappings = pluginPropertyMappings;
  }

  @Override
  public String toString() {
    return "TriggeringPropertyMapping{" +
      "argumentMappings=" + argumentMappings +
      ", pluginPropertyMappings=" + pluginPropertyMappings +
      '}';
  }
}
