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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.SecureStoreData;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.junit.Before;
import org.junit.Test;

public class FileSecureStoreDataTest {
  private static final String STORE_PATH = System.getProperty("java.io.tmpdir");

  private FileSecureStoreProvider fileSecureStoreProvider;
  private SecureStoreData secureStoreData;

  @Before
  public void setUp() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(Constants.Security.Store.SECURE_STORE_FILE_PATH, STORE_PATH);
    fileSecureStoreProvider = FileSecureStoreProvider.getInstance(conf);
  }

  @Test
  public void getMetadata() throws Exception {

  }

  @Test
  public void get() throws Exception {

  }
}
