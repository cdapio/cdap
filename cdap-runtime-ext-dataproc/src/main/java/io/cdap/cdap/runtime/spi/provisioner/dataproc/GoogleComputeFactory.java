/*
 *  Copyright © 2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class GoogleComputeFactory implements ComputeFactory {

  @Override
  public Compute createCompute(DataprocConf conf) throws GeneralSecurityException, IOException {
    GoogleCredentials credentials = conf.getComputeCredential();
    int connectTimeout = conf.getComputeConnectionTimeout();
    int readTimeout = conf.getComputeReadTimeout();

    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    HttpRequestInitializer timeoutRequestInitializer = httpRequest -> {
      requestInitializer.initialize(httpRequest);
      httpRequest.setConnectTimeout(connectTimeout);
      httpRequest.setReadTimeout(readTimeout);
    };
    return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(),
        timeoutRequestInitializer)
        .setApplicationName("cdap")
        .build();
  }
}
