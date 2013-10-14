package com.continuuity.common.http.core;

import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;

/**
 * HttpResponder is used to send response back to clients.
 */
public interface HttpResponder {

  /**
   * Sends json response back to the client.
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   */
  void sendJson(HttpResponseStatus status, Object object);

  /**
   * Sends json response back to the client.
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   * @param type Type of object.
   */
  void sendJson(HttpResponseStatus status, Object object, Type type);

  /**
   * Sends json response back to the client using the given gson object.
   * @param status Status of the response.
   * @param object Object that will be serialized into Json and sent back as content.
   * @param type Type of object.
   * @param gson Gson object for serialization.
   */
  void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson);

  /**
   * Send a string response back to the http client.
   * @param status status of the Http response.
   * @param data string data to be sent back.
   */
  void sendString(HttpResponseStatus status, String data);

  /**
   * Send only a status code back to client without any content.
   * @param status status of the Http response.
   */
  void sendStatus(HttpResponseStatus status);

  /**
   * Send a response containing raw bytes. Sets "application/octet-stream" as content type header.
   * @param status status of the Http response.
   * @param bytes bytes to be sent back.
   * @param headers headers to be sent back. This will overwrite any headers set by the framework.
   */
  void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers);

  /**
   * Sends a response containing raw bytes. Default content type is "application/octet-stream", but can be
   * overridden in the headers.
   * @param status status of the Http response
   * @param buffer bytes to send
   * @param headers Headers to send.
   */
  void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers);

  /**
   * Sends error message back to the client.
   *
   * @param status Status of the response.
   * @param errorMessage Error message sent back to the client.
   */
  void sendError(HttpResponseStatus status, String errorMessage);

  /**
   * Respond to the client saying the response will be in chunks. Add chunks to response using @{link sendChunk}
   * and @{link sendChunkEnd}.
   * @param status  the status code to respond with. Defaults to 200-OK if null.
   * @param headers additional headers to send with the response. May be null.
   */
  void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers);

  /**
   * Add a chunk of data to the response. @{link sendChunkStart} should be called before calling this method.
   * @{link sendChunkEnd} should be called after all chunks are done.
   * @param content the chunk of content to send
   */
  void sendChunk(ChannelBuffer content);

  /**
   * Called after all chunks are done. This keeps the connection alive
   * unless specified otherwise in the original request.
   */
  void sendChunkEnd();

  /**
   * Send response back to client.
   * @param status Status of the response.
   * @param content Content to be sent back.
   * @param contentType Type of content.
   * @param headers Headers to be sent back.
   */
  void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType,
                   Multimap<String, String> headers);


  /**
   * Sends a file content back to client.
   * @param file
   * @param headers
   */
  void sendFile(File file, Multimap<String, String> headers);
}
