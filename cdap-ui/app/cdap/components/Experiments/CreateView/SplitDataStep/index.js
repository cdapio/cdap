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

import PropTypes from 'prop-types';
import React from 'react';
import {connect, Provider} from 'react-redux';
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import {createSplitAndUpdateStatus, setSplitFinalized} from 'components/Experiments/store/CreateExperimentActionCreator';
import SplitInfo from 'components/Experiments/CreateView/SplitDataStep/SplitInfo';
import IconSVG from 'components/IconSVG';

require('./SplitDataStep.scss');

const renderSplitBtn = (splitInfo, onClick) => {
  let isSplitCreated = Object.keys(splitInfo).length;
  let isSplitComplete = (splitInfo || {}).status === 'Complete';
  if (!isSplitCreated || (isSplitCreated && isSplitComplete)) {
    return (
      <button
        className="btn btn-primary"
        onClick={onClick}
      >
        Split data Randomly and verify sample
      </button>
    );
  }
  return (
    <button
      className="btn btn-primary"
      disabled
    >
      <div className="btn-inner-container">
        <IconSVG name="icon-spinner" className="fa-spin" />
        <span>Splitting data</span>
      </div>
    </button>
  );
};

function SplitDataStep({splitInfo = {}, createSplitAndUpdateStatus, setSplitFinalized}) {
  let splitStatus = splitInfo.status || 'Complete';
  return (
    <div className="split-data-step">
      <h3>Split Data </h3>
      <div>Create Test Dataset for this Model.</div>
      <br />
      {renderSplitBtn(splitInfo, createSplitAndUpdateStatus)}
      {
        Object.keys(splitInfo).length && splitInfo.status === 'Complete' ? (
          <div className="action-button-group">
            <SplitInfo />
            <button
              className="btn btn-primary"
              onClick={setSplitFinalized}
              disabled={splitStatus !== 'Complete'}
            >
              Done
            </button>
            <span> Next, Select a machine learning algorithm </span>
          </div>
        ) : null
      }
    </div>
  );
}

SplitDataStep.propTypes = {
  splitInfo: PropTypes.object,
  schema: PropTypes.object,
  createSplitAndUpdateStatus: PropTypes.func,
  setSplitFinalized: PropTypes.func
};

const mapStateToSplitDataStepProps = (state) => {
  let {model_create} = state;
  let {splitInfo = {}} = model_create;
  return {
    splitInfo: splitInfo,
    schema: splitInfo.schema
  };
};
const mapDispatchToSplitDataStepProps = () => {
  return {
    createSplitAndUpdateStatus,
    setSplitFinalized
  };
};
const ConnectedSplitDataStep = connect(mapStateToSplitDataStepProps, mapDispatchToSplitDataStepProps)(SplitDataStep);
function ProvidedSplitDataStep() {
  return (
    <Provider store={createExperimentStore}>
      <ConnectedSplitDataStep />
    </Provider>
  );
}
export default ProvidedSplitDataStep;

