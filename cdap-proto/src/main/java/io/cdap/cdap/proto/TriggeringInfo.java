package io.cdap.cdap.proto;

import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public abstract class TriggeringInfo implements Trigger {
  private final Type type;
  private final ScheduleId scheduleId;
  @Nullable
  private final Map<String, String> runtimeArguments;

  protected TriggeringInfo(Type type, ScheduleId scheduleId, @Nullable Map<String, String> runtimeArguments) {
    this.type = type;
    this.scheduleId = scheduleId;
    this.runtimeArguments = runtimeArguments;
  }

  @Override
  public Type getType() {
    return type;
  }

  public ScheduleId getScheduleId() {
    return scheduleId;
  }

  @Nullable
  public Map<String, String> getRuntimeArguments() {
    return runtimeArguments;
  }

  public static class TimeTriggeringInfo extends TriggeringInfo {
    private final String cronExpression;

    public TimeTriggeringInfo(ScheduleId scheduleId, Map<String, String> runtimeArguments, String cronExpression) {
      super(Type.TIME, scheduleId, runtimeArguments);
      this.cronExpression = cronExpression;
    }

    public String getCronExpression() {
      return cronExpression;
    }
  }

  public static class ProgramStatusTriggeringInfo extends TriggeringInfo {
    private final ProgramRunId programRunId;

    public ProgramStatusTriggeringInfo(ScheduleId scheduleId, Map<String, String> runtimeArguments,
                                          ProgramRunId programRunId) {
      super(Type.PROGRAM_STATUS, scheduleId, runtimeArguments);
      this.programRunId = programRunId;
    }

    public ProgramRunId getProgramRunId() {
      return programRunId;
    }
  }

  public abstract static class AbstractCompositeTriggeringInfo<T extends TriggeringInfo> extends TriggeringInfo {
    private final List<T> triggeringInfos;
    private final TriggeringPropertyMapping triggeringPropertyMapping;

    protected AbstractCompositeTriggeringInfo(Type type, List<T> triggeringInfos, ScheduleId scheduleId,
                                              Map<String, String> runtimeArguments,
                                              TriggeringPropertyMapping triggeringPropertyMapping) {
      super(type, scheduleId, runtimeArguments);
      this.triggeringInfos = triggeringInfos;
      this.triggeringPropertyMapping = triggeringPropertyMapping;
    }

    public List<T> getTriggeringInfos() {
      return triggeringInfos;
    }

    public TriggeringPropertyMapping getTriggeringPropertyMapping() {
      return triggeringPropertyMapping;
    }
  }

  public static class OrTriggeringInfo extends AbstractCompositeTriggeringInfo<TriggeringInfo> {

    public OrTriggeringInfo(List<TriggeringInfo> triggeringInfos, ScheduleId scheduleId,
                            Map<String, String> runtimeArguments,
                            TriggeringPropertyMapping triggeringPropertyMapping) {
      super(Type.OR, triggeringInfos, scheduleId, runtimeArguments, triggeringPropertyMapping);
    }
  }

  public static class AndTriggeringInfo extends AbstractCompositeTriggeringInfo<TriggeringInfo> {

    public AndTriggeringInfo(List<TriggeringInfo> triggeringInfos, ScheduleId scheduleId,
                             Map<String, String> runtimeArguments,
                             TriggeringPropertyMapping triggeringPropertyMapping) {
      super(Type.AND, triggeringInfos, scheduleId, runtimeArguments, triggeringPropertyMapping);
    }
  }


}
