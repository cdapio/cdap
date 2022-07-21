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

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * A context for {@link AccessController} extensions to interact with CDAP. This context is available to
 * {@link AccessController} extensions in the {@link AccessController#initialize(AuthorizationContext)} method.
 *
 * Extensions can use this class to:
 * <ol>
 *   <li>Perform admin operations such as create/update/truncate/drop/exists on a dataset.</li>
 *   <li>Instantiate datasets and obtain objects for them.</li>
 *   <li>Execute operations on datasets inside transactions.</li>
 *   <li>Determine the authentication details of the {@link Principal} making the authorization request.</li>
 * </ol>
 */
public interface AuthorizationContext extends DatasetContext, Admin, Transactional, AuthenticationContext, SecureStore {
  /**
   * Returns the properties for the authorization extension. These properties are composed of all the properties
   * defined in {@code cdap-site.xml} with the prefix {@code security.authorization.extension.config.}.
   *
   * @return the {@link Properties} for the authorization extension
   */
  Properties getExtensionProperties();

  /**
   * Currently messaging is not supported. Calling this method always result in {@link UnsupportedOperationException}.
   */
  @Override
  void createTopic(String topic) throws TopicAlreadyExistsException, IOException;

  /**
   * Currently messaging is not supported. Calling this method always result in {@link UnsupportedOperationException}.
   */
  @Override
  void createTopic(String topic, Map<String, String> properties) throws TopicAlreadyExistsException, IOException;

  /**
   * Currently messaging is not supported. Calling this method always result in {@link UnsupportedOperationException}.
   */
  @Override
  Map<String, String> getTopicProperties(String topic) throws TopicNotFoundException, IOException;

  /**
   * Currently messaging is not supported. Calling this method always result in {@link UnsupportedOperationException}.
   */
  @Override
  void updateTopic(String topic, Map<String, String> properties) throws TopicNotFoundException, IOException;

  /**
   * Currently messaging is not supported. Calling this method always result in {@link UnsupportedOperationException}.
   */
  @Override
  void deleteTopic(String topic) throws TopicNotFoundException, IOException;
}
