/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataReaderContext;
import co.cask.cdap.api.metadata.MetadataWriterContext;
import com.google.inject.PrivateModule;

/**
 * Guice binding for {@link MetadataReaderContext} and {@link MetadataWriterContext} to be used in program containers.
 */
public class MetadataContextModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(MetadataAdmin.class).to(DefaultMetadataAdmin.class);
    expose(MetadataAdmin.class);
    // TODO: Bind to cloud implementation in cloud mode. How to check cdap is in cloud mode?
    bind(MetadataReaderContext.class).to(InPremMetadataReader.class);
    expose(MetadataReaderContext.class);
  }
}
