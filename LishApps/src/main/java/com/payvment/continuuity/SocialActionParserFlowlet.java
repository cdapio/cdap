package com.payvment.continuuity;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.payvment.continuuity.entity.SocialAction;

/**
 * Flowlet parses social actions from their JSON format into an internal
 * {@link SocialAction} object and then into a tuple to the next flowlet.
 */
public class SocialActionParserFlowlet extends AbstractFlowlet {

  static int numProcessed = 0;

  private OutputEmitter<SocialAction> socialActionOutputEmitter;

  /**
   * JSON parser
   */
  private final Gson gson = new Gson();

  @ProcessInput
  public void process(StreamEvent event) {
    String jsonEventString = new String(Bytes.toBytes(event.getBody()));

    // Perform any necessary pre-processing
    jsonEventString = preProcessSocialActionJSON(jsonEventString);

    // Parse social action JSON using GSON
    SocialAction action = null;
    try {
      action =
          this.gson.fromJson(jsonEventString, SocialAction.class);
      // Emit tuple
      this.socialActionOutputEmitter.emit(action);
    } catch (JsonParseException jpe) {
      throw jpe;
    } finally {
      numProcessed++;
    }
  }

  static String preProcessSocialActionJSON(String jsonEventString) {
    return jsonEventString.replaceFirst("@id", "id");
  }
}
