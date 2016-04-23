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

package co.cask.cdap.data.view;

import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.ViewNotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * {@link EntityExistenceVerifier} for {@link StreamViewId stream views}.
 */
public class ViewExistenceVerifier implements EntityExistenceVerifier<StreamViewId> {

  private final StreamAdmin streamAdmin;

  @Inject
  ViewExistenceVerifier(StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;
  }

  @Override
  public void ensureExists(StreamViewId viewId) throws StreamNotFoundException, ViewNotFoundException {
    boolean exists;
    try {
      exists = streamAdmin.viewExists(viewId.toId());
    } catch (StreamNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    if (!exists) {
      throw new ViewNotFoundException(viewId.toId());
    }
  }
}
