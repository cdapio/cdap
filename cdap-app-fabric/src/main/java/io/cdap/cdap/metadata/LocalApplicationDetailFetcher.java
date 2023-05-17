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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Fetch {@link ApplicationDetail} from local store via {@link ApplicationLifecycleService}.
 */
public class LocalApplicationDetailFetcher implements ApplicationDetailFetcher {

  private final ApplicationLifecycleService applicationLifecycleService;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public LocalApplicationDetailFetcher(ApplicationLifecycleService applicationLifecycleService,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    this.applicationLifecycleService = applicationLifecycleService;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Get {@link ApplicationDetail} for the given {@link ApplicationId}.
   *
   * @param appRef the versionless id of the application
   * @return {@link ApplicationDetail} for the given application
   * @throws IOException if failed to get {@link ApplicationDetail} for the given {@link
   *     ApplicationId}
   * @throws NotFoundException if the given the given application doesn't exist
   */
  @Override
  public ApplicationDetail get(ApplicationReference appRef) throws IOException, NotFoundException {
    try {
      return applicationLifecycleService.getLatestAppDetail(appRef);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, NotFoundException.class, IOException.class);
      throw new IOException(e);
    }
  }

  /**
   * Scans all the latest application details in the given namespace.
   */
  @Override
  public void scan(String namespace, Consumer<ApplicationDetail> consumer, Integer batchSize)
      throws IOException, NamespaceNotFoundException {
    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      // Check if the namespace exists before calling ApplicationLifecycleService, since it doesn't check
      // the existence of the namespace. Does a check here to explicitly throw an exception if nonexistent.
      if (!namespaceQueryAdmin.exists(namespaceId)) {
        throw new NamespaceNotFoundException(namespaceId);
      }
      applicationLifecycleService.scanApplications(namespaceId, ImmutableList.of(), consumer);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, NamespaceNotFoundException.class, IOException.class);
      throw new IOException(e);
    }
  }
}
