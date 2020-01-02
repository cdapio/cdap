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
import { Provider, connect } from 'react-redux';
import PipelineMetricsStore from 'services/PipelineMetricsStore';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.RunLevel';

const mapStateToProps = (state, ownProps) => {
  return {
    logsMetrics: state && state.logsMetrics ? state.logsMetrics : {},
    currentRun: ownProps.currentRun,
  };
};

const RunNumErrorsComp = ({ logsMetrics, currentRun }) => {
  return (
    <div className="run-info-container run-errors-container">
      <div>
        <strong>{T.translate(`${PREFIX}.errors`)}</strong>
      </div>
      <span>
        {currentRun && currentRun.starting ? `${logsMetrics['system.app.log.error'] || 0}` : '--'}
      </span>
    </div>
  );
};

RunNumErrorsComp.propTypes = {
  logsMetrics: PropTypes.object,
  currentRun: PropTypes.object,
};

const ConnectedRunNumErrors = connect(mapStateToProps)(RunNumErrorsComp);

export default function RunNumErrors({ currentRun }) {
  return (
    <Provider store={PipelineMetricsStore}>
      <ConnectedRunNumErrors currentRun={currentRun} />
    </Provider>
  );
}

RunNumErrors.propTypes = {
  currentRun: PropTypes.object,
};
