/*
 * Copyright 2014 Cask Data, Inc.
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
package co.cask.cdap.common.http;

import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;

/**
 * Convenient wrapper of {@link HttpResponse} that makes client code cleaner when dealing with java object that can be
 * de-serialized from response body.
 *
 * @param <T> type of the response object
 */
public final class ObjectResponse<T> extends HttpResponse {
  private static final Gson GSON = new Gson();

  private final T object;

  @SuppressWarnings("unchecked")
  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, Type typeOfObject, Gson gson) {
    T object = response.getResponseBody() == null ?
      null : (T) gson.fromJson(new String(response.getResponseBody(), Charsets.UTF_8), typeOfObject);
    return new ObjectResponse<T>(response, object);
  }

  @SuppressWarnings("unchecked")
  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, Type typeOfObject) {
    return fromJsonBody(response, typeOfObject, GSON);
  }

  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, TypeToken<T> typeOfObject, Gson gson) {
    return fromJsonBody(response, typeOfObject.getType(), gson);
  }

  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, TypeToken<T> typeOfObject) {
    return fromJsonBody(response, typeOfObject.getType(), GSON);
  }

  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, Class<T> typeOfObject, Gson gson) {
    return fromJsonBody(response, (Type) typeOfObject, gson);
  }

  public static <T> ObjectResponse<T> fromJsonBody(HttpResponse response, Class<T> typeOfObject) {
    return fromJsonBody(response, (Type) typeOfObject, GSON);
  }

  private ObjectResponse(HttpResponse response, T object) {
    super(response.getResponseCode(), response.getResponseMessage(), response.getResponseBody());
    this.object = object;
  }

  public T getResponseObject() {
    return object;
  }
}
