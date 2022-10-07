package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.TriggeringInfo;
import io.cdap.cdap.proto.TriggeringPropertyMapping;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper functions for {@link TriggeringInfo}
 */
public class TriggeringInfoHelper {

  private static Gson gson = new Gson();
  private static final String TRIGGERING_PROPERTIES_MAPPING = "triggering.properties.mapping";

  /**
   * Create a new {@link TriggeringInfo} object given
   * {@link DefaultTriggeringScheduleInfo}, {@link ScheduleId} and {@link Trigger.Type}
   * @param triggeringScheduleInfo
   * @param type
   * @param scheduleId
   * @return
   */
  public static TriggeringInfo fromTriggeringScheduleInfo(DefaultTriggeringScheduleInfo triggeringScheduleInfo,
                                                          Trigger.Type type, ScheduleId scheduleId) {
    List<TriggerInfo> triggerInfos = triggeringScheduleInfo.getTriggerInfos();
    switch(type) {
      case PROGRAM_STATUS:
        DefaultProgramStatusTriggerInfo programStatusTriggerInfo =
          (DefaultProgramStatusTriggerInfo) triggerInfos.get(0);
        return new TriggeringInfo.ProgramStatusTriggeringInfo(scheduleId,
                                                              programStatusTriggerInfo.getRuntimeArguments(),
                                                              getProgramRunId(programStatusTriggerInfo));
      case TIME:
        DefaultTimeTriggerInfo timeTriggerInfo = (DefaultTimeTriggerInfo) triggerInfos.get(0);
        return new TriggeringInfo.TimeTriggeringInfo(scheduleId, new HashMap<>(), timeTriggerInfo.getCronExpression());
      case OR:
        List<TriggeringInfo> triggeringInfos = getTriggeringInfoList(triggerInfos, scheduleId);
        return new TriggeringInfo.OrTriggeringInfo(triggeringInfos, scheduleId, null,
                                                   triggeringPropertyMapping(triggeringScheduleInfo.getProperties()));
      case AND:
        List<TriggeringInfo> triggeringInfos1 = getTriggeringInfoList(triggerInfos, scheduleId);
        return new TriggeringInfo.AndTriggeringInfo(triggeringInfos1, scheduleId, null,
                                                    triggeringPropertyMapping(triggeringScheduleInfo.getProperties()));
      case PARTITION:
        // TODO implement this
        return null;
      default:
        return null;
    }
  }

  private static ProgramRunId getProgramRunId(DefaultProgramStatusTriggerInfo programStatusTriggerInfo) {
    return new ProgramRunId(programStatusTriggerInfo.getNamespace(), programStatusTriggerInfo.getApplicationName(),
                            programTypeMappings.get(programStatusTriggerInfo.getProgramType()),
                            programStatusTriggerInfo.getProgram(), programStatusTriggerInfo.getRunId().getId());
  }

  private static TriggeringPropertyMapping triggeringPropertyMapping(Map<String, String> properties) {
    if (!properties.containsKey(TRIGGERING_PROPERTIES_MAPPING)) {
      return null;
    }
    return gson.fromJson(properties.get(TRIGGERING_PROPERTIES_MAPPING), TriggeringPropertyMapping.class);
  }

  /**
   * ProgramType mappings for {@link ProgramType} and {@link io.cdap.cdap.api.app.ProgramType}
   */
  static Map<io.cdap.cdap.api.app.ProgramType, ProgramType> programTypeMappings = new HashMap<>();

  static {
    programTypeMappings.put(io.cdap.cdap.api.app.ProgramType.WORKFLOW, ProgramType.WORKFLOW);
    programTypeMappings.put(io.cdap.cdap.api.app.ProgramType.MAPREDUCE, ProgramType.MAPREDUCE);
    programTypeMappings.put(io.cdap.cdap.api.app.ProgramType.SERVICE, ProgramType.SERVICE);
    programTypeMappings.put(io.cdap.cdap.api.app.ProgramType.SPARK, ProgramType.SPARK);
    programTypeMappings.put(io.cdap.cdap.api.app.ProgramType.WORKER, ProgramType.WORKER);
  }

  private static List<TriggeringInfo> getTriggeringInfoList(List<TriggerInfo> triggerInfos, ScheduleId scheduleId) {
    return triggerInfos.stream().map(r -> {
      DefaultProgramStatusTriggerInfo progTrigger = (DefaultProgramStatusTriggerInfo) r;
      return new TriggeringInfo.ProgramStatusTriggeringInfo(
        scheduleId, progTrigger.getRuntimeArguments(), getProgramRunId(progTrigger));
    }).collect(Collectors.toList());
  }

  /**
   * Serializer/Deserializer for {@link TriggeringInfo}
   */
  public static class TriggeringInfoCodec implements JsonDeserializer<TriggeringInfo>,
    JsonSerializer<TriggeringInfo> {

    private static final Map<Trigger.Type, Class<? extends TriggeringInfo>> TYPE_TO_TRIGGER_INFO =
      generateMap();

    private static Map<Trigger.Type, Class<? extends TriggeringInfo>> generateMap() {
      Map<Trigger.Type, Class<? extends TriggeringInfo>> map = new HashMap<>();
      map.put(Trigger.Type.AND, TriggeringInfo.AndTriggeringInfo.class);
      map.put(Trigger.Type.OR, TriggeringInfo.OrTriggeringInfo.class);
      map.put(Trigger.Type.PROGRAM_STATUS, TriggeringInfo.ProgramStatusTriggeringInfo.class);
      map.put(Trigger.Type.TIME, TriggeringInfo.TimeTriggeringInfo.class);
      return map;
    }

    private final Map<Trigger.Type, Class<? extends TriggeringInfo>> typeClassMap;

    public TriggeringInfoCodec() {
      this(TYPE_TO_TRIGGER_INFO);
    }

    protected TriggeringInfoCodec(Map<Trigger.Type, Class<? extends TriggeringInfo>> typeClassMap) {
      this.typeClassMap = typeClassMap;
    }

    @Override
    public TriggeringInfo deserialize(JsonElement json, Type type,
                                      JsonDeserializationContext context) throws JsonParseException {
      if (json == null) {
        return null;
      }
      if (!(json instanceof JsonObject)) {
        throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
      }
      JsonObject object = (JsonObject) json;
      JsonElement typeJson = object.get("type");
      Trigger.Type triggerType = context.deserialize(typeJson, Trigger.Type.class);
      Class<? extends TriggeringInfo> subClass = typeClassMap.get(triggerType);
      if (subClass == null) {
        throw new JsonParseException("Unable to map trigger type " + triggerType + " to a TriggerInfo class");
      }
      return context.deserialize(json, subClass);
    }

    @Override
    public JsonElement serialize(TriggeringInfo src, Type type, JsonSerializationContext context) {
      return context.serialize(src, src.getClass());
    }
  }


}
