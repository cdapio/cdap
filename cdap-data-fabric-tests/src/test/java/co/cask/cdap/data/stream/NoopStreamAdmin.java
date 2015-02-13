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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.StreamProperties;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

/**
 * A {@link StreamAdmin} that does nothing.
 */
public class NoopStreamAdmin implements StreamAdmin {

  @Override
  public void dropAll() throws Exception {
  }

  @Override
  public void configureInstances(QueueName streamName, long groupId, int instances) throws Exception {
  }

  @Override
  public void configureGroups(QueueName streamName, Map<Long, Integer> groupInfo) throws Exception {
  }

  @Override
  public void upgrade() throws Exception {
  }

  @Override
  public StreamConfig getConfig(String streamName) throws IOException {
    throw new IllegalStateException("Stream " + streamName + " not exists.");
  }

  @Override
  public void updateConfig(String streamName, StreamProperties properties) throws IOException {
  }

  @Override
  public long fetchStreamSize(StreamConfig streamConfig) throws IOException {
    return 0;
  }

  @Override
  public boolean exists(String name) throws Exception {
    return false;
  }

  @Override
  public void create(String name) throws Exception {
  }

  @Override
  public void create(String name, @Nullable Properties props) throws Exception {
  }

  @Override
  public void truncate(String name) throws Exception {
  }

  @Override
  public void drop(String name) throws Exception {
  }

  @Override
  public void upgrade(String name, Properties properties) throws Exception {
  }
}
