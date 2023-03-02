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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.master.spi.namespace.NamespaceDetail;
import io.cdap.cdap.master.spi.namespace.NamespaceListener;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.twill.api.TwillRunnerService;

/**
 * A service for initializing the {@link TwillRunnerService} with a list of namespaces.
 */
public class NamespaceInitializerService extends AbstractExecutionThreadService {

  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final TwillRunnerService twillRunnerService;

  @Inject
  NamespaceInitializerService(NamespaceQueryAdmin namespaceQueryAdmin,
      TwillRunnerService twillRunnerService) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  protected void run() throws Exception {
    if (!(twillRunnerService instanceof NamespaceListener)) {
      return;
    }
    List<NamespaceDetail> namespaceDetails = namespaceQueryAdmin.list().stream()
        .map(meta -> new NamespaceDetail(meta.getName(), meta.getConfig().getConfigs()))
        .collect(Collectors.toList());
    NamespaceListener namespaceListener = (NamespaceListener) twillRunnerService;
    namespaceListener.onStart(namespaceDetails);
  }
}
