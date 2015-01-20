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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.data.stream.StreamLeaderListener;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;

/**
 * This class is responsible for performing leader election amongst the Stream handlers. It also assigns for
 * each Stream handler a set of Streams to be the leader of.
 */
public interface StreamCoordinator extends Service {
  /**
   * Set the {@link Discoverable} that defines the Stream handler in which this {@link StreamCoordinator} runs.
   * This method has to be called before this service is started.
   *
   * @param discoverable discoverable that defines the Stream handler in which this object runs.
   */
  void setHandlerDiscoverable(Discoverable discoverable);

  /**
   * This method is called every time the Stream handler in which this {@link StreamCoordinator}
   * runs becomes the leader of a set of streams. Prior to this call, the Stream handler might
   * already have been the leader of some of those streams.
   *
   * @param callback {@link StreamLeaderListener} called when this Stream handler becomes leader
   *                 of a collection of streams
   * @return A {@link Cancellable} to cancel the watch
   */
  Cancellable addLeaderListener(StreamLeaderListener callback);
}
