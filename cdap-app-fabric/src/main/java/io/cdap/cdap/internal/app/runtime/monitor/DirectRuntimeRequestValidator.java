/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteRuntimeTable;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.netty.handler.codec.http.HttpRequest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RuntimeRequestValidator} implementation that reads from the runtime table directly.
 */
final class DirectRuntimeRequestValidator implements RuntimeRequestValidator {

  private final TransactionRunner transactionRunner;
  private final LoadingCache<ProgramRunId, Boolean> programRunsCache;

  DirectRuntimeRequestValidator(CConfiguration cConf, TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;

    // Configure the cache with expiry the poll time.
    // This helps reducing the actual lookup for a burst of requests within one poll interval,
    // but not to keep it too long so that data becomes stale.
    long pollTimeMillis = cConf.getLong(Constants.RuntimeMonitor.POLL_TIME_MS);
    this.programRunsCache = CacheBuilder.newBuilder()
      .expireAfterWrite(pollTimeMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<ProgramRunId, Boolean>() {
        @Override
        public Boolean load(ProgramRunId programRunId) throws IOException {
          return isValid(programRunId);
        }
      });
  }

  @Override
  public void validate(ProgramRunId programRunId, HttpRequest request) throws BadRequestException {
    boolean exists;
    try {
      exists = programRunsCache.get(programRunId);
    } catch (Exception e) {
      throw new ServiceUnavailableException(Constants.Service.RUNTIME, e);
    }
    if (!exists) {
      throw new BadRequestException("Program run " + programRunId + " is not valid");
    }
  }

  /**
   * Checks if the given {@link ProgramRunId} is valid.
   */
  private boolean isValid(ProgramRunId programRunId) throws IOException {
    return TransactionRunners.run(transactionRunner, context -> {
      return RemoteRuntimeTable.create(context).exists(programRunId);
    }, IOException.class);
  }
}
