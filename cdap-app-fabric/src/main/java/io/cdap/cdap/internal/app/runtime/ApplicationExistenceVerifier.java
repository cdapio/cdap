/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.proto.id.ApplicationId;

/**
 * {@link EntityExistenceVerifier} for {@link ApplicationId applications}.
 */
public class ApplicationExistenceVerifier implements EntityExistenceVerifier<ApplicationId> {
  private final Store store;

  @Inject
  ApplicationExistenceVerifier(Store store) {
    this.store = store;
  }

  @Override
  public void ensureExists(ApplicationId appId) throws ApplicationNotFoundException {
    if (store.getApplication(appId) == null) {
      throw new ApplicationNotFoundException(appId);
    }
  }
}
