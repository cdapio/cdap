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
import {Provider, connect} from 'react-redux';
import PipelineDetailStore from 'components/PipelineDetails/store';
import CurrentRunIndex from 'components/PipelineDetails/RunLevelInfo/CurrentRunIndex';
require('./RunLevelInfo.scss');

const mapStateToProps = (state) => {
  return {
    runs: state.runs,
    currentRun: state.currentRun
  };
};

const ConnectedCurrentRunIndex = connect(mapStateToProps)(CurrentRunIndex);

export default function RunLevelInfo() {
  return (
    <Provider store={PipelineDetailStore}>
      <div className="pipeline-details-run-level-info">
        <ConnectedCurrentRunIndex />
      </div>
    </Provider>
  );
}
