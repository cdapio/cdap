package io.cdap.cdap.spi.events.trigger;

import java.util.List;

public class TriggeringInfo {

  private final String namespace;
  private final String triggerName;
  private final List<ProgramEvent> programEvents;
  private final TriggeringPropertyMapping triggeringPropertyMapping;

  public TriggeringInfo(String namespace, String triggerName, List<ProgramEvent> programEvents,
                        TriggeringPropertyMapping triggeringPropertyMapping) {
    this.namespace = namespace;
    this.triggerName = triggerName;
    this.programEvents = programEvents;
    this.triggeringPropertyMapping = triggeringPropertyMapping;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTriggerName() {
    return triggerName;
  }

  public List<ProgramEvent> getProgramStatusEventDetails() {
    return programEvents;
  }

  public TriggeringPropertyMapping getTriggeringPropertyMapping() {
    return triggeringPropertyMapping;
  }

  @Override
  public String toString() {
    return "TriggeringInfo{" +
      "namespace='" + namespace + '\'' +
      ", triggerName='" + triggerName + '\'' +
      ", programEvents=" + programEvents +
      ", triggeringPropertyMapping=" + triggeringPropertyMapping +
      '}';
  }
}
