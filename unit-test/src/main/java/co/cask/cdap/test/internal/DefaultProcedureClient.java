/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.test.internal;

import co.cask.cdap.test.ProcedureClient;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Simple procedure client that uses java URLConnection to fire requests.
 */
public final class DefaultProcedureClient implements ProcedureClient {

  private final DiscoveryServiceClient discoveryServiceClient;
  private final String accountId;
  private final String applicationId;
  private final String procedureName;

  @Inject
  public DefaultProcedureClient(DiscoveryServiceClient discoveryServiceClient,
                                @Assisted("accountId") String accountId,
                                @Assisted("applicationId") String applicationId,
                                @Assisted("procedureName") String procedureName) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.accountId = accountId;
    this.applicationId = applicationId;
    this.procedureName = procedureName;
  }

  @Override
  public byte[] queryRaw(String method, Map<String, String> arguments) throws IOException {
    Discoverable discoverable = discoveryServiceClient.discover(
      String.format("procedure.%s.%s.%s",
                    accountId, applicationId, procedureName)).iterator().next();

    URL url = new URL(String.format("http://%s:%d/apps/%s/procedures/%s/methods/%s",
                      discoverable.getSocketAddress().getHostName(),
                      discoverable.getSocketAddress().getPort(),
                      applicationId,
                      procedureName,
                      method));
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoOutput(true);
    JsonWriter writer = new JsonWriter(new OutputStreamWriter(urlConn.getOutputStream(), Charsets.UTF_8));
    try {
      new Gson().toJson(arguments, new TypeToken<Map<String, String>>() { }.getType(), writer);
    } finally {
      writer.close();
    }
    if (urlConn.getResponseCode() != 200) {
      throw new IOException("Response code != 200 (responded = " +
                              urlConn.getResponseCode() + " " + urlConn.getResponseMessage() + ")");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(urlConn.getInputStream(), bos);
    return bos.toByteArray();
  }

  @Override
  public String query(String method, Map<String, String> arguments) throws IOException {
    return new String(queryRaw(method, arguments), Charsets.UTF_8);
  }
}
