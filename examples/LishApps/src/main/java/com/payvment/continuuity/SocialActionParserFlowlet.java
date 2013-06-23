/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
