/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.internal;

import co.cask.cdap.proto.Id;
import co.cask.cdap.test.StreamManager;
import com.google.inject.assistedinject.Assisted;

/**
 * Factory to create {@link StreamManager} objects
 */
public interface StreamManagerFactory {
  /**
   * Return a {@link StreamManager} for the specified stream
   *
   * @param streamId {@link Id.Stream} of the stream for which a {@link StreamManager} is requested
   * @return {@link StreamManager} for the specified stream
   */
  StreamManager create(@Assisted("streamId") Id.Stream streamId);
}
