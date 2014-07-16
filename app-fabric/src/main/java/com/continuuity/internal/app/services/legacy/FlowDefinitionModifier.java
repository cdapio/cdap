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

import java.net.URI;

/**
 * FlowDefinitionModifier interface is used for modifying the definition.
 * There are cases were there is a need to modify the definition to annotate
 * FlowDefinition with more information. This is used only by internal systems.
 */
public interface FlowDefinitionModifier  {
  /**
   * Sets stream URI
   *
   * @param flowlet name of the flowlet
   * @param stream name of the stream
   * @param uri URI associated with the stream.
   */
  public void setStreamURI(String flowlet, String stream, URI uri, StreamType type);
}
