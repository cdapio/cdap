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

package co.cask.cdap.security.store;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreManager;

import java.io.IOException;
import java.util.Map;

/**
 * A dummy class that is loaded when the user has set the provider to "kms" but the cluster does not
 * have the required libraries. All operations on this class throw an UnsupportedOperationException.
 */
public class DummySecureStore implements SecureStore, SecureStoreManager {

  private static final String SECURE_STORE_SETUP = "Secure store is not configured. To use secure store the provider " +
    "needs to be set using the \"security.store.provider\" property in cdap-site.xml. " +
    "The value could either be \"kms\" for Hadoop KMS based provider or \"file\" for Java JCEKS based provider. " +
    "Both without quotes. " +
    "KMS based provider is supported in distributed mode for Apache Hadoop 2.6.0 and up and on distribution versions " +
    "that are based on Apache Hadoop 2.6.0 and up. " +
    "Java JCEKS based provider is supported in In-Memory and Standalone modes. If the provider is set to file then " +
    "the user must provide the password to be used to access the keystore. The password can be set using " +
    "\"security.store.file.password\" property in cdap-security.xml. ";

  @Override
  public Map<String, String> listSecureData(String namespace) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description,
                            Map<String, String> properties) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }

  @Override
  public void deleteSecureData(String namespace, String name) throws IOException {
    throw new UnsupportedOperationException(SECURE_STORE_SETUP);
  }
}
