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

package io.cdap.cdap.internal.events;

import io.cdap.cdap.extension.AbstractExtensionLoader;

import java.util.Map;
import java.util.Set;

public class EventWriterExtensionProvider extends AbstractExtensionLoader<String, EventWriter>  implements EventWriterProvider{

  public EventWriterExtensionProvider(String extDirs) {
    super(extDirs);
  }

  @Override
  public Map<String, EventWriter> loadEventWriters() {
    return null;
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(EventWriter eventWriter) {
    return null;
  }
}
