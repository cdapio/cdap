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
 * Implementation of @link{FlowStreamDefinition}.
 */
public class FlowStreamDefinitionImpl implements FlowStreamDefinition {

  /** the name of the stream */
  String name;
  /** the URI of the stream */
  URI uri;

  /**
   * Empty constructor of this object.
   */
  public FlowStreamDefinitionImpl() {
  }

  /**
   * Constructor from name and uri
   * @param name the name of the stream
   * @param uri the URI of the stream
   */
  public FlowStreamDefinitionImpl(String name, URI uri) {
    this.name = name;
    this.uri = uri;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public URI getURI() {
    return uri;
  }
}
