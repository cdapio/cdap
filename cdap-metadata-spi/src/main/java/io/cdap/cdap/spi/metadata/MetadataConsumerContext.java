/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Beta;
import java.util.Map;

/**
 * Interface for an {@link MetadataConsumer} context.
 */
@Beta
public interface MetadataConsumerContext {

  /**
   * Returns the {@link Map} of properties associated with the Metadata Consumer extension. The
   * properties for the extension can be specified in the <code>cdap-site.xml</code> prefixed with
   * <code>metadata.consumer.</code>. The returned {@link Map} will have all such properties, but
   * with prefix <code>metadata.consumer.</code> stripped.
   *
   * @return the MetadataConsumer properties for the program run
   */
  Map<String, String> getProperties();

  /**
   * Returns a {@link MetadataConsumerMetrics} object configured based on {@code context}.
   */
  MetadataConsumerMetrics getMetrics(Map<String, String> context);
}
