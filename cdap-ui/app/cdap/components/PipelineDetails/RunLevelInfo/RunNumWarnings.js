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
import {Provider, connect} from 'react-redux';
import PipelineMetricsStore from 'services/PipelineMetricsStore';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.RunLevel';

const mapStateToProps = (state, ownProps) => {
  return {
    logsMetrics: state && state.logsMetrics ? state.logsMetrics : {},
    currentRun: ownProps.currentRun
  };
};

const RunNumWarningsComp = ({logsMetrics, currentRun}) => {
  return (
    <div className="run-info-container">
      <div>
        <strong>{T.translate(`${PREFIX}.warnings`)}</strong>
      </div>
      <span>
        {
          currentRun && currentRun.start ?
            `${logsMetrics['system.app.log.warn'] || 0}`
          :
            '--'
        }
      </span>
    </div>
  );
};

RunNumWarningsComp.propTypes = {
  logsMetrics: PropTypes.object,
  currentRun: PropTypes.object
};

const ConnectedRunNumWarnings = connect(mapStateToProps)(RunNumWarningsComp);

export default function RunNumWarnings({currentRun}) {
  return (
    <Provider store={PipelineMetricsStore}>
      <ConnectedRunNumWarnings currentRun={currentRun} />
    </Provider>
  );
}

RunNumWarnings.propTypes = {
  currentRun: PropTypes.object
};
