/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.gateway.auth;

import com.continuuity.common.conf.Constants;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements Authenticator {

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    return true;
  }

  @Override
  public boolean authenticateRequest(AvroFlumeEvent event) {
    return true;
  }

  @Override
  public String getAccountId(HttpRequest httpRequest) {
    return Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public String getAccountId(AvroFlumeEvent event) {
    return Constants.DEVELOPER_ACCOUNT_ID;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}
