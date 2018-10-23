/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { handleRunsPageChange } from 'components/Reports/store/ActionCreator';
import PaginationWithTitle from 'components/PaginationWithTitle';
import T from 'i18n-react';

const PREFIX = 'features.Reports.ReportsDetail';

function RunsPaginationView({ totalCount, offset, limit }) {
  let totalPages = Math.ceil(totalCount / limit);
  let currentPage;

  if (offset === 0) {
    currentPage = 1;
  } else {
    currentPage = Math.ceil((offset + 1) / limit);
  }

  return (
    <PaginationWithTitle
      handlePageChange={handleRunsPageChange}
      currentPage={currentPage}
      totalPages={totalPages}
      title={T.translate(`${PREFIX}.runs`, { context: totalCount })}
    />
  );
}

RunsPaginationView.propTypes = {
  totalCount: PropTypes.number,
  offset: PropTypes.number,
  limit: PropTypes.number,
};

const mapStateToProps = (state) => {
  return {
    totalCount: state.details.totalRunsCount,
    offset: state.details.runsOffset,
    limit: state.details.runsLimit,
  };
};

const RunsPagination = connect(mapStateToProps)(RunsPaginationView);

export default RunsPagination;
