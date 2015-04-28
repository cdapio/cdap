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

package co.cask.cdap.logging.gateway.handlers.store;

import co.cask.cdap.data2.dataset2.tx.DatasetContext;
import co.cask.cdap.data2.dataset2.tx.Transactional;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.RunRecord;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * Uses AppMetadataStore to access meta data.
 */
public class ProgramStore {
  private final Transactional<DatasetContext<AppMetadataStore>, AppMetadataStore> txnl;

  @Inject
  public ProgramStore(final AppMdsTableUtil tableUtil, TransactionExecutorFactory txExecutorFactory) {
    this.txnl = Transactional.of(txExecutorFactory, new Supplier<DatasetContext<AppMetadataStore>>() {
      @Override
      public DatasetContext<AppMetadataStore> get() {
        try {
          return DatasetContext.of(new AppMetadataStore(tableUtil.getMetaTable()));
        } catch (Exception e) {
          // there's nothing much we can do here
          throw Throwables.propagate(e);
        }
      }
    });
  }

  /**
   * Returns run record for a given run.
   *
   * @param id program id
   * @param runid run id
   * @return run record for runid
   */
  public RunRecord getRun(final Id.Program id, final String runid) {
    return txnl.executeUnchecked(new TransactionExecutor.Function<DatasetContext<AppMetadataStore>, RunRecord>() {
      @Override
      public RunRecord apply(DatasetContext<AppMetadataStore> context) throws Exception {
        return context.get().getRun(id, runid);
      }
    });
  }
}
