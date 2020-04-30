/*
 * Copyright © 2018-2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.common;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.compute.Compute;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterStatus;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Abstract Dataproc client to share common methods between Dataproc and Existing Dataproc implementations
 */
public abstract class AbstractDataprocClient {
  public static final String DATAPROC_GOOGLEAPIS_COM_443 = "-dataproc.googleapis.com:443";

  /**
   * Extracts and returns the zone name from the given full zone URI.
   */
  public static String getZone(String zoneUri) {
    int idx = zoneUri.lastIndexOf("/");
    if (idx <= 0) {
      throw new IllegalArgumentException("Invalid zone uri " + zoneUri);
    }
    return zoneUri.substring(idx + 1);
  }

  /*
   * Converts Google Dataproc cluster status to Datafusion Cluster Status
   */
  public io.cdap.cdap.runtime.spi.provisioner.ClusterStatus convertStatus(ClusterStatus status) {
    switch (status.getState()) {
      case ERROR:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.FAILED;
      case RUNNING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      case CREATING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.CREATING;
      case DELETING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.DELETING;
      case UPDATING:
        // not sure if this is correct, or how it can get to updating state
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      default:
        // unrecognized and unknown
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.ORPHANED;
    }
  }

  /*
   * Using the input Google Credentials retrieve the Dataproc Cluster controller client
   */
  public static ClusterControllerClient getClusterControllerClient(GoogleCredentials credentials, String region)
    throws IOException {
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);

    String regionalEndpoint = region + DATAPROC_GOOGLEAPIS_COM_443;

    ClusterControllerSettings controllerSettings = ClusterControllerSettings.newBuilder()
      .setCredentialsProvider(credentialsProvider)
      .setEndpoint(regionalEndpoint)
      .build();
    return ClusterControllerClient.create(controllerSettings);
  }

  /*
   * Retrieve Google Compute Instance using Credentials
   */
  public static Compute getCompute(GoogleCredential credential) throws GeneralSecurityException, IOException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    return new Compute.Builder(httpTransport, JacksonFactory.getDefaultInstance(), credential)
      .setApplicationName("cdap")
      .build();
  }

}
