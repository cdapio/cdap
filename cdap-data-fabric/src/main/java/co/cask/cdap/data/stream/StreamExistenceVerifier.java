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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * {@link EntityExistenceVerifier} for {@link StreamId streams}.
 */
public class StreamExistenceVerifier implements EntityExistenceVerifier<StreamId> {

  private final StreamAdmin streamAdmin;

  @Inject
  StreamExistenceVerifier(StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;
  }

  @Override
  public void ensureExists(StreamId streamId) throws StreamNotFoundException {
    boolean exists;
    try {
      exists = streamAdmin.exists(streamId.toId());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    if (!exists) {
      throw new StreamNotFoundException(streamId.toId());
    }
  }
}
