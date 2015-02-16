/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * This class responsible for process coordination needed between stream writers and consumers.
 */
public interface StreamCoordinatorClient extends Service {

  /**
   * Receives event for changes in stream properties.
   *
   * @param streamId name of the stream
   * @param listener listener to get called when there is change in stream properties.
   * @return A {@link Cancellable} to cancel the watch
   */
  Cancellable addListener(Id.Stream streamId, StreamPropertyListener listener);

  /**
   * Creates a stream by performing the given action. The execution of the action is protected by a lock
   * so that it is guaranteed that there is only one thread executing the given action for the given stream
   * across the whole system.
   *
   * @param streamId name of the stream
   * @param action action to perform. If a new stream is created, it should returns a {@link StreamConfig} representing
   *               the configuration of the new stream; otherwise {@code null} should be returned.
   * @return The {@link StreamConfig} as returned by the action
   * @throws Exception if the action throws Exception
   */
  @Nullable
  StreamConfig createStream(Id.Stream streamId, Callable<StreamConfig> action) throws Exception;

  /**
   * Updates the stream properties by performing the given action. The execution of the action is protected by a lock
   * so that it is guaranteed that there is only one thread executing the given action for the given stream
   * across the whole system.
   *
   * @param streamId name of the stream
   * @param action action to perform. It should returns a {@link CoordinatorStreamProperties} containing information
   *               about the update properties.
   * @throws Exception if failed to update properties
   */
  void updateProperties(Id.Stream streamId, Callable<CoordinatorStreamProperties> action) throws Exception;
}
