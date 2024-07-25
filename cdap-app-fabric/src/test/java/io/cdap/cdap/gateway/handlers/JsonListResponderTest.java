/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.common.io.ByteBuffers;
import io.cdap.http.ChunkResponder;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class JsonListResponderTest {

  @Test
  public void testSendForPaginatedListResponder() throws IOException {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    ChunkResponder chunkResponder = Mockito.mock(ChunkResponder.class);
    Mockito.when(responder.sendChunkStart(HttpResponseStatus.OK)).thenReturn(chunkResponder);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Mockito.doAnswer(invocation -> {
      ByteBuffers.writeToStream(invocation.getArgument(0, ByteBuffer.class), byteArrayOutputStream);
      return null;
    }).when(chunkResponder).sendChunk(Mockito.any(ByteBuffer.class));

    JsonPaginatedListResponder.respond(
        new Gson(), responder, "applications", (jsonListResponder) -> {
          jsonListResponder.send("application");
          return "nextToken";
        });

    JsonParser parser = new JsonParser();
    JsonObject json = (JsonObject) parser.parse(byteArrayOutputStream.toString());
    Assert.assertEquals(json.get("applications").getAsString(), "application");
    Assert.assertEquals(json.get("nextPageToken").getAsString(), "nextToken");
  }

  @Test
  public void testMultipleSendForPaginatedListResponder() throws IOException {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    ChunkResponder chunkResponder = Mockito.mock(ChunkResponder.class);
    Mockito.when(responder.sendChunkStart(HttpResponseStatus.OK)).thenReturn(chunkResponder);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Mockito.doAnswer(invocation -> {
      ByteBuffers.writeToStream(invocation.getArgument(0, ByteBuffer.class), byteArrayOutputStream);
      return null;
    }).when(chunkResponder).sendChunk(Mockito.any(ByteBuffer.class));

    JsonPaginatedListResponder.respond(
        new Gson(), responder, "applications", (jsonListResponder) -> {
          jsonListResponder.send("application0");
          jsonListResponder.send("application1");
          return "nextToken";
        });

    JsonParser parser = new JsonParser();
    JsonObject json = (JsonObject) parser.parse(byteArrayOutputStream.toString());
    Assert.assertEquals(json.get("applications").getAsJsonArray().get(0).getAsString(), "application0");
    Assert.assertEquals(json.get("applications").getAsJsonArray().get(1).getAsString(), "application1");
    Assert.assertEquals(json.get("nextPageToken").getAsString(), "nextToken");
  }

  @Test
  public void testSendForWholeListResponder() throws IOException {
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    ChunkResponder chunkResponder = Mockito.mock(ChunkResponder.class);
    Mockito.when(responder.sendChunkStart(HttpResponseStatus.OK)).thenReturn(chunkResponder);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    Mockito.doAnswer(invocation -> {
      ByteBuffers.writeToStream(invocation.getArgument(0, ByteBuffer.class), byteArrayOutputStream);
      return null;
    }).when(chunkResponder).sendChunk(Mockito.any(ByteBuffer.class));

    JsonWholeListResponder.respond(
        new Gson(), responder, (jsonListResponder) -> {
          jsonListResponder.send("application");
        });

    JsonParser parser = new JsonParser();
    JsonArray array = (JsonArray) parser.parse(byteArrayOutputStream.toString());
    Assert.assertEquals(array.get(0).getAsString(), "application");
  }

}
