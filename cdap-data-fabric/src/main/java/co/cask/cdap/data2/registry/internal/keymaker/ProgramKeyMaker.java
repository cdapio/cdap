/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.registry.internal.keymaker;

import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.data2.registry.internal.pair.KeyMaker;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;

/**
 * {@link KeyMaker} for {@link ProgramId}.
 */
public class ProgramKeyMaker implements KeyMaker<ProgramId> {

  /**
   * Creates a {@link ProgramId} with no program name. This allows matching
   * for all datasets associated with an Application as opposed to a single program.
   */
  public static ProgramId getProgramId(ApplicationId applicationId) {
    // Use empty programId to denote applicationId
    return applicationId.flow("");
  }

  @Override
  public MDSKey getKey(ProgramId programId) {
    MDSKey.Builder keyBuilder = new MDSKey.Builder()
      .add(programId.getNamespace())
      .add(programId.getApplication());

    // If programId is empty, this is actually applicationId
    if (!programId.getEntityName().isEmpty()) {
      keyBuilder.add(programId.getType().getCategoryName());
      keyBuilder.add(programId.getEntityName());
    }

    return keyBuilder.build();
  }

  @Override
  public void skipKey(MDSKey.Splitter splitter) {
    splitter.skipString(); // namespace
    splitter.skipString(); // app
    splitter.skipString(); // type
    splitter.skipString(); // program
  }

  @Override
  public ProgramId getElement(MDSKey.Splitter splitter) {
    return new ProgramId(splitter.getString(), splitter.getString(),
                           ProgramType.valueOfCategoryName(splitter.getString()),
                           splitter.getString());
  }
}
