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

import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Interface for fetching {@code ApplicationDetail}
 */
public interface ApplicationDetailFetcher {

  /**
   * Get the application detail for the given application reference
   *
   * @param appRef the versionless ID of the application
   * @return the detail of the given application
   * @throws IOException if failed to get {@code ApplicationDetail}
   * @throws NotFoundException if the application or namespace identified by the supplied id
   *     doesn't exist
   */
  ApplicationDetail get(ApplicationReference appRef)
      throws IOException, NotFoundException, UnauthorizedException;

  /**
   * Scans all the latest version of application details in the given namespace
   *
   * @param namespace the namespace to scan application details from
   * @param consumer a {@link Consumer} to consume each ApplicationDetail being scanned
   * @param batchSize the number of application details to be scanned in each batch
   */
  void scan(String namespace, Consumer<ApplicationDetail> consumer, Integer batchSize)
      throws IOException, NamespaceNotFoundException, UnauthorizedException;
}
