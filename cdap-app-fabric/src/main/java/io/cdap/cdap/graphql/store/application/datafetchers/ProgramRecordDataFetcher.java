/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.application.datafetchers;

import com.google.inject.Inject;
import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramType;

import java.util.List;

/**
 * Fetchers to get program records
 */
public class ProgramRecordDataFetcher {

  private static final ProgramRecordDataFetcher INSTANCE = new ProgramRecordDataFetcher();

  @Inject
  private ProgramRecordDataFetcher() {
  }

  public static ProgramRecordDataFetcher getInstance() {
    return INSTANCE;
  }

  /**
   * Fetcher to get program records. Used to filter by type of record
   *
   * @return the data fetcher
   */
  public DataFetcher getProgramRecordsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        ApplicationDetail applicationDetail = dataFetchingEnvironment.getSource();
        List<ProgramRecord> programs = applicationDetail.getPrograms();

        String type = dataFetchingEnvironment.getArgument(ApplicationFields.TYPE);

        if(type == null) {
          return programs;
        }

        ProgramType programType = ProgramType.valueOfPrettyName(type);
        programs.removeIf(programRecord -> programRecord.getType().equals(programType));

        return programs;
      }
    );
  }
}
