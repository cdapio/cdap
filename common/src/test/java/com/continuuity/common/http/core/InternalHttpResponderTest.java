package com.continuuity.common.http.core;

import com.google.common.base.Charsets;
import com.google.common.collect.HashMultimap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class InternalHttpResponderTest {

  @Test
  public void testSendJson() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    JsonObject output = new JsonObject();
    output.addProperty("data", "this is some data");
    responder.sendJson(HttpResponseStatus.OK, output);

    InternalHttpResponse response = responder.getResponse();
    assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusCode());
    JsonObject responseData = new Gson().fromJson(
      new InputStreamReader(response.getInputSupplier().getInput()), JsonObject.class);
    assertEquals(output, responseData);
  }

  @Test
  public void testSendString() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendString(HttpResponseStatus.BAD_REQUEST, "bad request");

    validateResponse(responder.getResponse(), HttpResponseStatus.BAD_REQUEST, "bad request");
  }

  @Test
  public void testSendStatus() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendStatus(HttpResponseStatus.NOT_FOUND);

    validateResponse(responder.getResponse(), HttpResponseStatus.NOT_FOUND, null);
  }

  @Test
  public void testSendByteArray() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendByteArray(
      HttpResponseStatus.OK, "abc".getBytes(Charsets.UTF_8), HashMultimap.<String, String>create());

    validateResponse(responder.getResponse(), HttpResponseStatus.OK, "abc");
  }

  @Test
  public void testSendError() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendError(HttpResponseStatus.NOT_FOUND, "not found");

    validateResponse(responder.getResponse(), HttpResponseStatus.NOT_FOUND, "not found");
  }

  @Test
  public void testChunks() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendChunkStart(HttpResponseStatus.OK, HashMultimap.<String, String>create());
    responder.sendChunk(ChannelBuffers.wrappedBuffer("a".getBytes(Charsets.UTF_8)));
    responder.sendChunk(ChannelBuffers.wrappedBuffer("b".getBytes(Charsets.UTF_8)));
    responder.sendChunk(ChannelBuffers.wrappedBuffer("c".getBytes(Charsets.UTF_8)));
    responder.sendChunkEnd();

    validateResponse(responder.getResponse(), HttpResponseStatus.OK, "abc");
  }

  @Test
  public void testSendContent() throws IOException {
    InternalHttpResponder responder = new InternalHttpResponder();
    responder.sendContent(HttpResponseStatus.OK, ChannelBuffers.wrappedBuffer("abc".getBytes(Charsets.UTF_8)),
                          "contentType", HashMultimap.<String, String>create());

    validateResponse(responder.getResponse(), HttpResponseStatus.OK, "abc");
  }

  private void validateResponse(InternalHttpResponse response, HttpResponseStatus expectedStatus, String expectedData)
    throws IOException {
    int code = response.getStatusCode();
    assertEquals(expectedStatus.getCode(), code);
    if (expectedData != null) {
      // read it twice to make sure the input supplier gives the full stream more than once.
      for (int i = 0; i < 2; i++) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputSupplier().getInput()));
        try {
          String data = reader.readLine();
          assertEquals(expectedData, data);
        } finally {
          reader.close();
        }
      }
    }
  }
}
