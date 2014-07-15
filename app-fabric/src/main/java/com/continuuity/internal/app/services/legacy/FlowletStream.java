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
 * Helper class to building flowlet queue.
 */
public final class FlowletStream {

  private static final Logger Log = LoggerFactory.getLogger(FlowStream.class);

  /**
   * Build the default URI for an out stream of a flowlet,
   * which is queue://flowname/flowletname/streamname.
   * @param flowName the name of the flow
   * @param flowletName the name of the flowlet
   * @param streamName the name of the flowlet output stream
   * @return the URI for the stream
   */
  public static URI defaultURI(String flowName, String flowletName, String streamName) {
    try {
      return new URI("queue", flowName, "/" + flowletName + "/" + streamName, null);
    } catch (URISyntaxException e) {
      Log.error("Cannot construct a valid URI from flow id {}, flowlet name {} and stream name {}: {}",
          new Object[] { flowName, flowletName, streamName, e.getMessage() });
      throw new IllegalArgumentException("Invalid flow id '"
          + flowName + "', flowlet name '" + flowletName + "' and stream name '" + streamName + "'.", e);
    }
  }
}
