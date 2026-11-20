/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * RemoteValidationRequest
 */
public class RemoteValidationRequest {

  private final String namespace;
  //The original request string
  private final String request;
  private final Set<String> disabledTypes;

  public RemoteValidationRequest(String namespace, String request) {
    this(namespace, request, Collections.emptySet());
  }

  public RemoteValidationRequest(String namespace, String request,
      @Nullable Set<String> disabledTypes) {
    this.namespace = namespace;
    this.request = request;
    this.disabledTypes = disabledTypes == null
        ? Collections.emptySet() : Collections.unmodifiableSet(disabledTypes);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getRequest() {
    return request;
  }

  public Set<String> getDisabledTypes() {
    return disabledTypes == null ? Collections.emptySet() : disabledTypes;
  }
}
