/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class CaseInsensitiveEnumTypeAdapterFactoryTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
    .create();

  @Test
  public void testDeserialization() {
    Assert.assertEquals(Suit.CLUB, GSON.fromJson("club", Suit.class));
    Assert.assertEquals(Suit.CLUB, GSON.fromJson("CLUB", Suit.class));
    Assert.assertEquals(Suit.CLUB, GSON.fromJson("cLub", Suit.class));
  }

  @Test
  public void testSerialization() {
    Assert.assertEquals("\"club\"", GSON.toJson(Suit.CLUB));
  }

  private enum Suit {
    CLUB,
    DIAMOND,
    HEART,
    SPADE
  }
}
