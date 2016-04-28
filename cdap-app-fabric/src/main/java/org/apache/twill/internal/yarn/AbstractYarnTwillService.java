/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.yarn;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.AbstractTwillService;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Abstract class for implementing {@link com.google.common.util.concurrent.Service} that runs in
 * YARN container which provides methods to handle secure token updates.
 */
public abstract class AbstractYarnTwillService extends AbstractTwillService {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractYarnTwillService.class);
  protected final Location applicationLocation;
  protected volatile Credentials credentials;

  protected AbstractYarnTwillService(ZKClient zkClient, RunId runId, Location applicationLocation) {
    super(zkClient, runId);
    this.applicationLocation = applicationLocation;
  }

  /**
   * Returns the location of the secure store, or {@code null} if either not running in secure mode or an error
   * occur when trying to acquire the location.
   */
  protected final Location getSecureStoreLocation() {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return null;
    }
    try {
      return applicationLocation.append(Constants.Files.CREDENTIALS);
    } catch (IOException e) {
      LOG.error("Failed to create secure store location.", e);
      return null;
    }
  }

  /**
   * Attempts to handle secure store update.
   *
   * @param message The message received
   * @return {@code true} if the message requests for secure store update, {@code false} otherwise.
   */
  protected final boolean handleSecureStoreUpdate(Message message) {
    if (!SystemMessages.SECURE_STORE_UPDATED.equals(message)) {
      return false;
    }

    // If not in secure mode, simply ignore the message.
    if (!UserGroupInformation.isSecurityEnabled()) {
      return true;
    }

    try {
      Credentials credentials = new Credentials();
      Location location = getSecureStoreLocation();
      try (DataInputStream input = new DataInputStream(new BufferedInputStream(location.getInputStream()))) {
        credentials.readTokenStorageStream(input);
      }

      UserGroupInformation.getCurrentUser().addCredentials(credentials);
      this.credentials = credentials;

      LOG.info("Secure store updated from {}.", location);

    } catch (Throwable t) {
      LOG.error("Failed to update secure store.", t);
    }

    return true;
  }
}
