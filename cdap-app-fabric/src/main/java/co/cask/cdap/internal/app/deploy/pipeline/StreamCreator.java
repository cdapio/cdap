/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * Creates streams.
 */
final class StreamCreator {
  private final StreamAdmin streamAdmin;
  private static final Logger LOG = LoggerFactory.getLogger(StreamCreator.class);

  StreamCreator(StreamAdmin streamAdmin) {
    this.streamAdmin = streamAdmin;
  }

  /**
   * Create the given streams and the Hive tables for the streams if explore is enabled.
   *
   * @param namespaceId the namespace to have the stream created in
   * @param streamSpecs the set of stream specifications for streams to be created
   * @param ownerPrincipal the principal of the stream owner if one exists else null
   * @param authorizingUser the authorizing user who will be making the call
   * @throws Exception if there was an exception creating a stream
   */
  void createStreams(final NamespaceId namespaceId, Iterable<StreamSpecification> streamSpecs,
                     @Nullable KerberosPrincipalId ownerPrincipal, String authorizingUser) throws Exception {
    for (final StreamSpecification spec : streamSpecs) {
      final Properties props = new Properties();
      if (spec.getDescription() != null) {
        props.put(Constants.Stream.DESCRIPTION, spec.getDescription());
      }
      if (ownerPrincipal != null) {
        props.put(Constants.Security.PRINCIPAL, ownerPrincipal.getPrincipal());
      }
      AuthorizationUtil.authorizeAs(authorizingUser, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (streamAdmin.create(namespaceId.stream(spec.getName()), props) != null) {
            LOG.info("Stream '{}.{}' created successfully.", namespaceId.getNamespace(), spec.getName());
          }
          return null;
        }
      });
    }
  }
}
