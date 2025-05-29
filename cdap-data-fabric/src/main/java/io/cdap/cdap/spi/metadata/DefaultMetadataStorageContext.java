/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.spi.metadata;

import io.cdap.cdap.common.conf.CConfiguration;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Default implementation of the {@link MetadataStorageContext}.
 * TODO(CDAP-21174): Generalize the context for metadata storage
 */
public class DefaultMetadataStorageContext implements MetadataStorageContext {

  private final Map<String, String> properties;

  DefaultMetadataStorageContext(CConfiguration cConf, @Nullable String prefix) {
    if (prefix != null) {
      this.properties = Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
    } else {
      this.properties = Collections.emptyMap();
    }
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }
}

