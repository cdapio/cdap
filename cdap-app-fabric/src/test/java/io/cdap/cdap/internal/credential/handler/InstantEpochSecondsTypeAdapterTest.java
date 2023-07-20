/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.handler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link InstantEpochSecondsTypeAdapter}.
 */
public class InstantEpochSecondsTypeAdapterTest {

  @Test
  public void testInstantProducesExpectedTimestamp() {
    long expectedTime = 1689640167L;
    Gson gson = new GsonBuilder().registerTypeAdapter(Instant.class,
        new InstantEpochSecondsTypeAdapter()).create();
    String returnedTime = gson.toJson(Instant.ofEpochSecond(expectedTime));
    Assert.assertEquals(String.valueOf(expectedTime), returnedTime);
  }

  @Test
  public void testTimestampStringSerializesToExpectedInstant() {
    long expectedTime = 1689640237L;
    Gson gson = new GsonBuilder().registerTypeAdapter(Instant.class,
        new InstantEpochSecondsTypeAdapter()).create();
    Instant returnedInstant = gson.fromJson(String.valueOf(expectedTime), Instant.class);
    Assert.assertEquals(Instant.ofEpochSecond(expectedTime), returnedInstant);
  }
}
