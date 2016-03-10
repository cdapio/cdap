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
package co.cask.cdap.data2.dataset2;

import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.proto.Id;

/**
 * Implementation of the {@link ForwardingDatasetFramework} which is also {@link ProgramContextAware}.
 */
public class ForwardingProgramContextAwareDatasetFramework extends ForwardingDatasetFramework
  implements ProgramContextAware {

  public ForwardingProgramContextAwareDatasetFramework(DatasetFramework datasetFramework) {
    super(datasetFramework);
  }

  @Override
  public void initContext(Id.Run run) {
    if (delegate instanceof ProgramContextAware) {
      ((ProgramContextAware) delegate).initContext(run);
    }
  }

  @Override
  public void initContext(Id.Run run, Id.NamespacedId componentId) {
    if (delegate instanceof ProgramContextAware) {
      ((ProgramContextAware) delegate).initContext(run, componentId);
    }
  }
}
