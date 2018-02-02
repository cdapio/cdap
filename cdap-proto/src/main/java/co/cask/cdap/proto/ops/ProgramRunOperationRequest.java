/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.ops;

import java.util.List;

/**
 * Represents the request for a program run report or a dashboard detail in an HTTP request.
 */
public class ProgramRunOperationRequest {
  private final long startTs;
  private final long endTs;
  private final String principal;
  private final List<String> namespaces;

  public ProgramRunOperationRequest(long startTs, long endTs, String principal, List<String> namespaces) {
    this.startTs = startTs;
    this.endTs = endTs;
    this.principal = principal;
    this.namespaces = namespaces;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public String getPrincipal() {
    return principal;
  }

  public List<String> getNamespaces() {
    return namespaces;
  }
}
