/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.cdap.security.guice.preview;

import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.proto.security.SecurityContext;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Secure store to be used for preview. Reads are delegated but writes should happen in memory
 */
public class PreviewSecureStore implements SecureStore, SecureStoreManager {

  private final SecureStore delegate;
  /**
   * A hack to make the security context to use for a preview run available in downstream Spark and Hadoop. This is
   * necessary because Hadoop/Spark classes may create their own threads, so we guarantee the security context is
   * present at time of use.
   */
  private SecurityContext securityContext;

  public PreviewSecureStore(SecureStore delegate) {
    this.delegate = delegate;
  }

  public void setSecurityContext(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    SecurityRequestContext.set(securityContext);
    try {
      return delegate.list(namespace);
    } finally {
      SecurityRequestContext.reset();
    }
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    SecurityRequestContext.set(securityContext);
    try {
      return delegate.get(namespace, name);
    } finally {
      SecurityRequestContext.reset();
    }
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
                  Map<String, String> properties) throws Exception {
    //TODO put data in in-mempry map
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    // TODO delete the data from in-memory map if its present otherwise it would be no-op since we do not want to
    // delegate it
  }
}
