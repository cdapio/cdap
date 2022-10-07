package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.guava.reflect.TypeToken;

import java.lang.reflect.Type;

public class WorkflowTokenCodec implements JsonDeserializer<WorkflowToken> {

  @Override
  public WorkflowToken deserialize(JsonElement jsonElement, Type type,
                                   JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    return jsonDeserializationContext.deserialize(jsonElement, new TypeToken<BasicWorkflowToken>() { }.getType());
  }
}
