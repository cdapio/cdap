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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.inject.Inject;

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
    if (store.getApplication(appId.toId()) == null) {
      throw new ApplicationNotFoundException(appId.toId());
    }
  }
}
