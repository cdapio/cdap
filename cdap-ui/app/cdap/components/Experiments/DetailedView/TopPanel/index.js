/*
 * Copyright © 2017 Cask Data, Inc.
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
import {connect} from 'react-redux';
import TopPanel from 'components/Experiments/TopPanel';
import ExperimentMetricsDropdown from 'components/Experiments/DetailedView/ExperimentMetricsDropdown';


require('./DetailedViewTopPanel.scss');


const Metadata = ({name, description, srcpath, total, outcome}) => {
  return (
    <div className="experiment-metadata">
      <div>
        <h2 title={name}>{name}</h2>
        <div
          className="description"
          title={description}
        >
          {description}
        </div>
      </div>
      <div>
        <strong>
          <div> # Models </div>
          <h1>{total}</h1>
        </strong>
      </div>
      <div>
        <div>
          <strong className="experiment-meta-label">Test Data</strong>
          <div>{srcpath}</div>
        </div>
        <div>
          <strong className="experiment-meta-label">Outcome</strong>
          <div>{outcome}</div>
        </div>
      </div>
    </div>
  );
};

Metadata.propTypes = {
  name: PropTypes.string,
  description: PropTypes.string,
  srcpath: PropTypes.string,
  total: PropTypes.number,
  outcome: PropTypes.string
};

const mapStateToMetadataProps = (state) => {
  return {
    name: state.name,
    description: state.description,
    srcpath: state.srcpath,
    models: state.models,
    total: state.modelsTotalCount,
    outcome: state.outcome
  };
};

const ConnectedMetadata = connect(mapStateToMetadataProps)(Metadata);

const DetailedTopPanel = () => {
  return (
    <TopPanel className="detailed-view">
      <div className="experiment-toppanel-container">
        <ConnectedMetadata />
        <ExperimentMetricsDropdown />
      </div>
    </TopPanel>
  );
};

export default DetailedTopPanel;
