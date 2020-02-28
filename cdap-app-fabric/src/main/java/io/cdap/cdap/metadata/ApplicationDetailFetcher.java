/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;

import java.io.IOException;
import java.util.List;

/**
 * Interface for fetching {@code ApplicationDetail}
 */
public interface ApplicationDetailFetcher {

  /**
   * Get the application detail for the given application id
   * @param appId the id of the application
   * @return the detail of the given application
   * @throws IOException if failed to get {@code ApplicationDetail}
   * @throws NotFoundException if the application or namespace identified by the supplied id doesn't exist
   */
  ApplicationDetail get(ApplicationId appId) throws IOException, NotFoundException;

  /**
   * Get details of all applications in the given namespace
   * @param namespace the name of the namespace to get the list of applications
   * @return a list of {@code ApplicationDetail} in the given namspace
   * @throws IOException if failed to get the list of {@code ApplicationDetail}
   * @throws NamespaceNotFoundException if the given namespace doesn't exist
   */
  List<ApplicationDetail> list(String namespace) throws IOException, NamespaceNotFoundException;
}
