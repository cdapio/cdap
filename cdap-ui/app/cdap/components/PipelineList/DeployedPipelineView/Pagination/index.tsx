/*
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

import { connect } from 'react-redux';
import PaginationView from 'components/PipelineList/PaginationView';
import { setPage } from 'components/PipelineList/DeployedPipelineView/store/ActionCreator';
const mapStateToProps = (state) => {
  let { filteredPipelines = [], pipelines = [] } = state.deployed;
  const { pageLimit, currentPage } = state.deployed;
  /**
   * We need to show pagination if,
   * 1. The current filtered pipelines length is 1 less than pageLimit, or
   * 2. The current filtered pipelines is less than total pipelines, or
   * 3. If the user is in any page other than 1 (which means we need to show always)
   *
   * The filteredPipelines will atmost have pageLimit pipelines. We don't need to show
   * pagination if there are exactly 25 pipelines or less (reason for pipelines & filteredPipelines length check).
   */
  filteredPipelines = filteredPipelines || [];
  pipelines = pipelines || [];
  const shouldDisplay =
    (filteredPipelines.length > pageLimit - 1 && pipelines.length > filteredPipelines.length) ||
    state.deployed.currentPage !== 1;
  return {
    currentPage,
    pageLimit,
    shouldDisplay,
    numPipelines: pipelines.length,
  };
};

const mapDispatch = () => {
  return {
    setPage,
  };
};

const Pagination = connect(
  mapStateToProps,
  mapDispatch
)(PaginationView);

export default Pagination;
