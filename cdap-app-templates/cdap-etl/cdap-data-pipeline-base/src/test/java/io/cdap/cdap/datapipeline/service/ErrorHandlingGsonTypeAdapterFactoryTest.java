/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.datapipeline.oauth.RefreshTokenResponse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for ErrorHandlingGsonTypeAdapterFactory
 */
public class ErrorHandlingGsonTypeAdapterFactoryTest {
  private static final Gson GSON = new GsonBuilder()
    .setPrettyPrinting()
    .registerTypeAdapterFactory(new ErrorHandlingGsonTypeAdapterFactory())
    .create();

  // Ensure parsing doesn't throw when one of the items is not a String
  @Test
  public void refreshTokenParseTest() {
    String json = "{" +
        "  \"access_token\": \"asdf1234\"," +
        "  \"scope\": [1, 2, 3]" +
        "}";
    RefreshTokenResponse refreshTokenResponse = GSON.fromJson(json, RefreshTokenResponse.class);

    Assert.assertEquals(refreshTokenResponse.getAccessToken(), "asdf1234");
    Assert.assertNull(refreshTokenResponse.getScope());
  }
}
