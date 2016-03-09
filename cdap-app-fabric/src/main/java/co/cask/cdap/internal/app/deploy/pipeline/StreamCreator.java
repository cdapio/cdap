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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.proto.Id;

import java.util.Set;

/**
 * Creates streams.
 */
public class StreamCreator {

  private final Id.Namespace namespace;
  private final StreamAdmin streamAdmin;

  public StreamCreator(Id.Namespace namespace, StreamAdmin streamAdmin) {
    this.namespace = namespace;
    this.streamAdmin = streamAdmin;
  }

  /**
   * Create the given streams and the Hive tables for the streams if explore is enabled.
   *
   * @param streamNames the set of streams to create
   * @throws Exception if there was an exception creating a stream
   */
  public void createStreams(Set<String> streamNames) throws Exception {
    for (String streamName : streamNames) {
      streamAdmin.create(Id.Stream.from(namespace, streamName));
    }
  }
}
