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

package io.cdap.cdap.common.io;

import com.google.gson.stream.JsonReader;
import io.cdap.cdap.format.io.JsonDecoder;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class StringCachingDecoderTest {

  @Test
  public void testNoDuplicates() throws IOException {
    JsonReader jsonReader = new JsonReader(new StringReader(
      "'a''a''a'"));
    jsonReader.setLenient(true);
    StringCachingDecoder decoder = new StringCachingDecoder(new JsonDecoder(jsonReader), new HashMap<>());
    String first = decoder.readString();
    String second = decoder.readString();
    Assert.assertSame(first, second);
    decoder.getCache().clear();
    //Now we should get a different object with a fresh cache
    String third = decoder.readString();
    Assert.assertEquals(first, third);
    Assert.assertNotSame(first, third);
  }
}
