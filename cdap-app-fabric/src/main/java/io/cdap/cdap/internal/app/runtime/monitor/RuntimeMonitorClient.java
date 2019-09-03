/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Supplier;
import javax.net.ssl.HttpsURLConnection;

/**
 * Provides client side logic to interact with the network API exposed by {@link RuntimeMonitorServer}.
 */
public final class RuntimeMonitorClient {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeMonitorClient.class);

  private final HttpRequestConfig requestConfig;
  private final HttpsEnabler httpsEnabler;
  private final DatumReader<GenericRecord> responseDatumReader;
  private final Supplier<InetSocketAddress> serverAddressSupplier;
  private final Proxy proxy;
  private volatile URI baseURI;

  public RuntimeMonitorClient(HttpRequestConfig requestConfig, KeyStore keyStore, KeyStore trustStore,
                              Supplier<InetSocketAddress> serverAddressSupplier, Proxy proxy) {
    this.requestConfig = requestConfig;
    this.httpsEnabler = new HttpsEnabler().setKeyStore(keyStore, ""::toCharArray).setTrustStore(trustStore);
    this.responseDatumReader = new GenericDatumReader<>(
      MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType().getElementType());
    this.serverAddressSupplier = serverAddressSupplier;
    this.proxy = proxy;
  }

  /**
   * Fetches messages from the server side by providing the given request.
   *
   * @param request a map from topic config key to {@link MonitorConsumeRequest}
   * @return a map from topic config key to {@link Deque} of {@link MonitorMessage} in the same order as the
   *         server responded
   * @throws IOException if failed to fetch messages from the server
   * @throws IllegalArgumentException if server responded with 400 Bad Request
   * @throws ServiceUnavailableException if the runtime monitor server is not available
   */
  Map<String, Deque<MonitorMessage>> fetchMessages(Map<String, MonitorConsumeRequest> request) throws IOException {
    HttpURLConnection urlConn = connect("runtime/metadata");
    try {
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("POST");
      urlConn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "avro/binary");

      try (OutputStream os = urlConn.getOutputStream()) {
        encodeRequest(request, os);
      }

      throwIfNotOK(urlConn.getResponseCode(), urlConn);
      return decodeResponse(urlConn.getInputStream());
    } catch (ConnectException e) {
      throw new ServiceUnavailableException("runtime.monitor", e);
    } finally {
      releaseConnection(urlConn);
    }
  }


  /**
   * Requests the runtime monitor server to shutdown.
   *
   * @throws IOException if failed to issue the command to the server
   * @throws IllegalArgumentException if server responded with 400 Bad Request
   * @throws ServiceUnavailableException if the runtime monitor server is not available
   */
  void requestShutdown() throws IOException {
    postCall("runtime/shutdown");
  }

  /**
   * Requests graceful termination of the running program.
   *
   * @throws IOException if failed to issue the command to the server
   * @throws IllegalArgumentException if server responded with 400 Bad Request
   * @throws ServiceUnavailableException if the runtime monitor server is not available
   */
  void requestStop() throws IOException {
    postCall("runtime/kill");
  }

  /**
   * Make a POST call to the given path with body.
   */
  private void postCall(String path) throws IOException {
    HttpURLConnection urlConn = connect(path);
    try {
      urlConn.setRequestMethod("POST");
      throwIfNotOK(urlConn.getResponseCode(), urlConn);
    } catch (ConnectException e) {
      throw new ServiceUnavailableException("runtime.monitor", e);
    } finally {
      releaseConnection(urlConn);
    }
  }

  /**
   * Encode request to avro binary format.
   * @param topicsToRequest topic requests to be
   * @param outputStream Outputstream to write to
   * @throws IOException if there is any exception while reading from output stream
   */
  private void encodeRequest(Map<String, MonitorConsumeRequest> topicsToRequest,
                             OutputStream outputStream) throws IOException {
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
    encoder.writeMapStart();
    encoder.setItemCount(topicsToRequest.size());

    DatumWriter<GenericRecord> requestDatumWriter = new GenericDatumWriter<>(
      MonitorSchemas.V1.MonitorConsumeRequest.SCHEMA.getValueType());

    for (Map.Entry<String, MonitorConsumeRequest> requestEntry : topicsToRequest.entrySet()) {
      encoder.startItem();
      encoder.writeString(requestEntry.getKey());
      requestDatumWriter.write(requestEntry.getValue().toGenericRecord(), encoder);
    }

    encoder.writeMapEnd();
  }

  /**
   * Decodes avro binary response.
   * @param is input stream to read from
   * @return Returns decoded map of messages per topic
   */
  private Map<String, Deque<MonitorMessage>> decodeResponse(InputStream is) {
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);

    Map<String, Deque<MonitorMessage>> decodedMessages = new HashMap<>();

    try {
      long entries = decoder.readMapStart();
      while (entries > 0) {
        for (int i = 0; i < entries; i++) {
          String topicConfig = decoder.readString();
          if (topicConfig.isEmpty()) {
            continue;
          }

          decodedMessages.put(topicConfig, decodeMessages(decoder));
        }

        entries = decoder.mapNext();
      }
    } catch (IOException e) {
      // catch the exception to process all the decoded messages to avoid refetching them.
      LOG.error("Error while decoding response from Runtime Server. ", e);
    }

    return decodedMessages;
  }

  /**
   * Decode monitor messages from decoder
   * @param decoder Decoder to decode messages
   * @return list of decoded monitor messages
   * @throws IOException if failed to decode the message
   */
  private Deque<MonitorMessage> decodeMessages(Decoder decoder) throws IOException {
    Deque<MonitorMessage> decodedMessages = new LinkedList<>();
    long messages = decoder.readArrayStart();
    while (messages > 0) {
      GenericRecord reuse = new GenericData.Record(MonitorSchemas.V1.MonitorResponse.SCHEMA.getValueType()
                                                     .getElementType());
      for (int j = 0; j < messages; j++) {
        reuse = responseDatumReader.read(reuse, decoder);
        decodedMessages.add(new MonitorMessage(reuse));
      }

      messages = decoder.arrayNext();
    }

    return decodedMessages;
  }

  /**
   * Throws exception if the given response code is not 200 OK.
   */
  private void throwIfNotOK(int responseCode, HttpURLConnection urlConn) throws IOException {
    switch (responseCode) {
      case HttpURLConnection.HTTP_OK:
        return;
      case HttpURLConnection.HTTP_BAD_REQUEST:
        throw new IllegalArgumentException(readError(urlConn));
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException("runtime.monitor", readError(urlConn));
      default:
        throw new IOException("Failed to talk to runtime monitor. Response code is "
                                + responseCode + ". Error is " + readError(urlConn));
    }
  }

  /**
   * Resolves the URL to the runtime monitor server for the given path.
   */
  private URL resolveURL(String path) throws IOException {
    if (baseURI == null) {
      InetSocketAddress addr = serverAddressSupplier.get();
      if (addr == null) {
        throw new IOException("No runtime monitor server address");
      }
      baseURI = URI.create("https://" + addr.getHostString() + ":" + addr.getPort() + "/v1/");
    }
    return baseURI.resolve(path).toURL();
  }

  /**
   * Connects to the given relative path of the base URI.
   */
  private HttpURLConnection connect(String path) throws IOException {
    URL url = resolveURL(path);
    URLConnection urlConn = url.openConnection(proxy);
    if (!(urlConn instanceof HttpsURLConnection)) {
      // This should not happen since we always connect with https
      throw new IOException("Connection is not secure");
    }

    urlConn.setConnectTimeout(requestConfig.getConnectTimeout());
    urlConn.setReadTimeout(requestConfig.getReadTimeout());
    return httpsEnabler.enable((HttpsURLConnection) urlConn);
  }

  /**
   * Releases the {@link HttpURLConnection} so that it can be reused.
   */
  private void releaseConnection(HttpURLConnection urlConn) {
    // Have to drain the InputStream before closing in order to have the connection reused
    try (InputStream is = urlConn.getInputStream()) {
      ByteStreams.toByteArray(is);
    } catch (Exception e) {
      // Ignore exception. We just want to release resources
    }
    try (InputStream is = urlConn.getErrorStream()) {
      // Error stream might be null if there is no error
      if (is != null) {
        ByteStreams.toByteArray(is);
      }
    } catch (Exception e) {
      // Ignore exception. We just want to release resources
    }
    urlConn.disconnect();
  }

  /**
   * Reads the whole error stream from the given {@link HttpURLConnection}.
   */
  private String readError(HttpURLConnection urlConn) {
    try (Reader reader = new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8)) {
      return CharStreams.toString(reader);
    } catch (IOException e) {
      // Ignored. Just return a hardcoded string.
      return "Unable to read error due to " + e.getMessage();
    }
  }
}
