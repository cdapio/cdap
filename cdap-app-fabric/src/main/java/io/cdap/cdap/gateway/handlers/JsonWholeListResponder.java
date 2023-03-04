/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.gateway.handlers;

import com.google.gson.Gson;
import io.cdap.http.HttpResponder;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Writes a json array with values provided by generator in a streaming fashion.
 */
public class JsonWholeListResponder extends JsonListResponder {

  private JsonWholeListResponder(Gson gson, HttpResponder responder) {
    super(gson, responder);
  }

  /**
   * Allows to write a json array response in a streaming fashion. Provided generator should call
   * {@link JsonListResponder#send(Object)} to write Json representation to an output stream.
   *
   * @param gson instance of Gson library to be used for json conversions
   * @param responder {@link HttpResponder} to be used to stream Json objects to
   * @param generator should call send for every generated object to be written to an output
   *     stream.
   */
  public static void respond(Gson gson, HttpResponder responder,
      Consumer<JsonListResponder> generator) throws IOException {

    JsonListResponder jsonListResponder = new JsonWholeListResponder(gson, responder);
    generator.accept(jsonListResponder);
    jsonListResponder.finish();
  }

  @Override
  protected void startResponse() throws IOException {
    jsonWriter.beginArray();
  }

  @Override
  protected void finishResponse() throws IOException {
    jsonWriter.endArray();
  }
}
