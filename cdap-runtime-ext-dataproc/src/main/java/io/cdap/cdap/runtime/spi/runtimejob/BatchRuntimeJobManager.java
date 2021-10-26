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

package io.cdap.cdap.runtime.spi.runtimejob;

import io.cdap.cdap.runtime.spi.ProgramRunInfo;

import java.util.List;
import java.util.Optional;

/**
 *
 */
public class BatchRuntimeJobManager implements RuntimeJobManager {


  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {

  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception {
    return Optional.empty();
  }

  @Override
  public List<RuntimeJobDetail> list() throws Exception {
    return null;
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {

  }

  @Override
  public void kill(ProgramRunInfo programRunInfo) throws Exception {

  }

  @Override
  public void close() {

  }
}
