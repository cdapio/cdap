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

import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.app.preview.PreviewStatus;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.twill.api.logging.LogEntry;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of the {@link PreviewManager}.
 */
public class DefaultPreviewManager implements PreviewManager {
  @Override
  public ApplicationId start(NamespaceId namespaceId, AppRequest<?> request) throws Exception {
    return null;
  }

  @Override
  public PreviewStatus getStatus(ApplicationId preview) throws NotFoundException {
    return null;
  }

  @Override
  public void stop(ApplicationId preview) throws Exception {

  }

  @Override
  public List<String> getTracers(ApplicationId preview) throws NotFoundException {
    return null;
  }

  @Override
  public Map<String, Map<String, List<Object>>> getData(ApplicationId preview) throws NotFoundException {
    return null;
  }

  @Override
  public Collection<MetricTimeSeries> getMetrics(ApplicationId preview) throws NotFoundException {
    return null;
  }

  @Override
  public List<LogEntry> getLogs(ApplicationId preview) throws NotFoundException {
    return null;
  }
}
