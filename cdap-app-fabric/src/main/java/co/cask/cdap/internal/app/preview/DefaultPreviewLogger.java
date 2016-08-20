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

package co.cask.cdap.internal.app.preview;

import co.cask.cdap.api.preview.PreviewLogger;
import co.cask.cdap.app.store.PreviewStore;
import co.cask.cdap.proto.id.PreviewId;

/**
 * Default implementation of {@link PreviewLogger}.
 */
public class DefaultPreviewLogger implements PreviewLogger {
  private final PreviewStore previewStore;
  private final PreviewId previewId;
  private final String loggerName;

  public DefaultPreviewLogger(PreviewStore previewStore, PreviewId previewId, String loggerName) {
    this.previewStore = previewStore;
    this.previewId = previewId;
    this.loggerName = loggerName;
  }

  @Override
  public void log(String propertyName, Object propertyValue) {
    previewStore.put(previewId, loggerName, propertyName, propertyValue);
  }
}
