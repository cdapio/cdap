/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data.runtime;

import com.continuuity.tephra.persist.HDFSTransactionStateStorage;
import com.continuuity.tephra.snapshot.SnapshotCodecProvider;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;

/**
 * Google Guice Provider for the {@link HDFSTransactionStateStorage} class.  Each call to {@link #get()} will return
 * a new instance of the storage provider.
 */
public class HDFSTransactionStateStorageProvider implements Provider<HDFSTransactionStateStorage> {
  private final Configuration hConf;
  private final SnapshotCodecProvider codecProvider;

  @Inject
  public HDFSTransactionStateStorageProvider(Configuration hConf, SnapshotCodecProvider codecProvider) {
    this.hConf = hConf;
    this.codecProvider = codecProvider;
  }

  @Override
  public HDFSTransactionStateStorage get() {
    return new HDFSTransactionStateStorage(hConf, codecProvider);
  }
}
