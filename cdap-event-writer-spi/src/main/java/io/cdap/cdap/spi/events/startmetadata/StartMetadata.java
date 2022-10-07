package io.cdap.cdap.spi.events.startmetadata;

import io.cdap.cdap.spi.events.trigger.TriggeringInfo;

public class StartMetadata {
  private final StartType type;
  private final TimeSchedule timeSchedule;
  private final TriggeringInfo triggeringInfo;

  public StartMetadata(StartType type, TimeSchedule timeSchedule, TriggeringInfo triggeringInfo) {
    this.type = type;
    this.timeSchedule = timeSchedule;
    this.triggeringInfo = triggeringInfo;
  }

  public StartType getType() {
    return type;
  }

  public TimeSchedule getTimeSchedule() {
    return timeSchedule;
  }

  public TriggeringInfo getTriggeringInfo() {
    return triggeringInfo;
  }

  @Override
  public String toString() {
    return "StartMetadata{" +
      "type=" + type +
      ", timeSchedule=" + timeSchedule +
      ", triggeringInfo=" + triggeringInfo +
      '}';
  }
}
