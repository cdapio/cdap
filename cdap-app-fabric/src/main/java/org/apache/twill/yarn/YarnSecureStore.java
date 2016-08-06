/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.yarn;

import com.google.common.base.Throwables;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.SecureStore;

import java.io.IOException;

/**
 * A {@link SecureStore} for hadoop credentials.
 * TODO: Copied from Twill 0.7 for CDAP-CDAP-6609.
 * TODO: Remove this after the fix is moved to Twill (TWILL-189).
 */
public final class YarnSecureStore implements SecureStore {

  private final Credentials credentials;
  private final UserGroupInformation ugi;

  public static SecureStore create(Credentials credentials) {
    try {
      return create(credentials, UserGroupInformation.getCurrentUser());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static SecureStore create(Credentials credentials, UserGroupInformation userGroupInformation) {
    return new YarnSecureStore(credentials, userGroupInformation);
  }

  private YarnSecureStore(Credentials credentials, UserGroupInformation ugi) {
    this.credentials = credentials;
    this.ugi = ugi;
  }

  @Override
  public Credentials getStore() {
    return credentials;
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }
}
