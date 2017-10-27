/*
 * Copyright © 2016 Cask Data, Inc.
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

const ExperimentMetadataWrapper = ({modelName, modelDescription, directives}) => {
  return (
    <div className="experiment-metadata">
      <div>
        <strong>Model Name: </strong>
        <span>{modelName}</span>
        <div>
          <strong>Model Description: </strong>
          <span>{modelDescription}</span>
        </div>
      </div>
      <div>
        <strong>No of Directives: </strong>
        <span>{directives.length}</span>
      </div>
      <div>
        <strong>Split Method: </strong>
        <span>Random</span>
      </div>
      <div className="grayed">
        <strong>ML Algorithm: </strong>
        <span>--</span>
      </div>
    </div>
  );
};
ExperimentMetadataWrapper.propTypes = {
  modelName: PropTypes.string,
  modelDescription: PropTypes.string,
  directives: PropTypes.array
};
const mapStateToProps = (state) => ({
  modelName: state.model_create.name,
  modelDescription: state.model_create.description,
  directives: state.model_create.directives
});
const ConnectedExperimentMetadata = connect(mapStateToProps, null)(ExperimentMetadataWrapper);

export default function ExperimentMetadata() {
  return (
    <Provider store={createExperimentStore}>
      <ConnectedExperimentMetadata />
    </Provider>
  );
}
