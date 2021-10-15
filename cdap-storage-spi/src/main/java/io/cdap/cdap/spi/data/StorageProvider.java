/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.spi.data.transaction.TransactionRunner;

/**
 * Interface for storage provider to implement for exposing storage capability.
 */
public interface StorageProvider extends AutoCloseable {

  /**
   * Initialization of the storage provide. This method will be called after the constructor
   * and before any method is being called.
   *
   * @param context a context object providing interaction with the CDAP platform.
   * @throws Exception if the storage provider failed to initialize itself
   */
  default void initialize(StorageProviderContext context) throws Exception {
    // no-op
  }

  /**
   * Returns the name of this storage provider. The name needs to match with the configuration provided through
   * {@code data.storage.implementation}.
   */
  String getName();

  /**
   * Returns the {@link StructuredTableAdmin} implementation for this storage.
   */
  StructuredTableAdmin getStructuredTableAdmin() throws Exception;

  /**
   * Returns the {@link TransactionRunner} implementation for performing transactional operations for this storage.
   */
  TransactionRunner getTransactionRunner() throws Exception;

  /**
   * Release any resources held by this storage provider.
   */
  @Override
  default void close() throws Exception {
    // no-op
  }
}
