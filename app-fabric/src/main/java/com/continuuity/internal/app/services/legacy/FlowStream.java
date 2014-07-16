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

package com.continuuity.internal.app.services.legacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Helper class to build stream uri for flow.
 */
public final class FlowStream {

  private static final Logger Log = LoggerFactory.getLogger(FlowStream.class);

  /**
   * Build the default URI for a stream, which is:
   *   stream://accountname/streamname
   * @param accountName the name of the account
   * @param streamName the name of the stream
   * @return the URI for the stream
   */
  public static URI buildStreamURI(String accountName, String streamName) {
    try {
      return new URI("stream", accountName, "/" + streamName, null);
    } catch (URISyntaxException e) {
      Log.error("Cannot construct a valid URI from account name {} and stream" +
          " name {}: {}",
          new Object[] { accountName, streamName, e.getMessage() });
      throw new IllegalArgumentException("Cannot construct URI from account " +
          "name "
          + accountName + " and stream name " + streamName + ".", e);
    }
  }
}
