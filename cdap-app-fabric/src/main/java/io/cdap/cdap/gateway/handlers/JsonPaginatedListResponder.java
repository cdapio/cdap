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
import java.util.function.Function;

/**
 * Writes a json object that contains an array with provided key and nextPageToken, returned from
 * {@link #respond} generator
 */
public class JsonPaginatedListResponder extends JsonListResponder {

  public static final String NEXT_PAGE_TOKEN_KEY = "nextPageToken";
  private final String valuesKey;
  private String nextPageToken;

  private JsonPaginatedListResponder(Gson gson, HttpResponder responder, String valuesKey) {
    super(gson, responder);

    this.valuesKey = valuesKey;
  }

  /**
   * Allows to write a paginated Json response. Provided generator should call {@link
   * JsonListResponder#send(Object)} to write Json representation to an output stream. It should
   * also return nextPageToken or null if its a last page.
   *
   * @param gson instance of Gson library to be used for json conversions
   * @param responder {@link HttpResponder} to be used to stream Json objects to
   * @param valuesKey json object key for values array
   * @param generator should call send for every generated object to be written to an output
   *     stream. Should return nextPageToken or null if its a last page.
   */
  public static void respond(Gson gson, HttpResponder responder,
      String valuesKey, Function<JsonListResponder, String> generator) throws IOException {
    JsonPaginatedListResponder jsonPaginatedListResponder = new JsonPaginatedListResponder(gson,
        responder,
        valuesKey);
    String nextPageToken = generator.apply(jsonPaginatedListResponder);
    jsonPaginatedListResponder.setNextPageToken(nextPageToken);
    jsonPaginatedListResponder.finish();
  }

  @Override
  protected void startResponse() throws IOException {
    jsonWriter.beginObject();
    jsonWriter.name(valuesKey);
    jsonWriter.beginArray();
  }

  @Override
  protected void finishResponse() throws IOException {
    jsonWriter.endArray();

    if (nextPageToken != null) {
      jsonWriter.name(NEXT_PAGE_TOKEN_KEY);
      jsonWriter.value(nextPageToken);
    }

    jsonWriter.endObject();
  }

  private void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }
}
