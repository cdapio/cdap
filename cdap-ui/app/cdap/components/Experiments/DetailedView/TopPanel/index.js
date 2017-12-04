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
import d3 from 'd3';
import PieChartWithLegends from 'components/PieChartWithLegend';
import {getAlgorithmLabel} from 'components/Experiments/store/ActionCreator';

require('./DetailedViewTopPanel.scss');
const HEIGHT_OF_PIE_CHART = 150;

const colorScale = d3.scale.category20();

const getAlgoDistribution = (models) => {
  if (!models.length) {
    return [];
  }
  let modelsMap = {};
  models.forEach(model => {
    let algo = model.algorithm;
    if (!modelsMap[algo]) {
      modelsMap = {
        ...modelsMap,
        [algo]: {
          value: getAlgorithmLabel(algo),
          count: 1,
          color: colorScale(algo)
        }
      };
    } else {
      modelsMap = {
        ...modelsMap,
        [algo]: {
          ...modelsMap[algo],
          count: modelsMap[algo].count + 1
        }
      };
    }
  });
  return Object.keys(modelsMap).map(m => modelsMap[m]);
};

const Metadata = ({name, description, srcpath, total, deployed, outcome}) => {
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
        <strong>
          <div> # Deployed </div>
          <h1>{deployed}</h1>
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
  total: PropTypes.string,
  deployed: PropTypes.string,
  outcome: PropTypes.string
};

const mapStateToMetadataProps = (state) => {
  return {
    name: state.name,
    description: state.description,
    srcpath: state.srcpath,
    models: state.models,
    total: state.models.length,
    deployed: state.models.filter(model => model.deployTime).length,
    outcome: state.outcome
  };
};

const ConnectedMetadata = connect(mapStateToMetadataProps)(Metadata);

const AlgorithmDistribution = ({models}) => {
  return (
    <PieChartWithLegends
      data={getAlgoDistribution(models)}
      width={HEIGHT_OF_PIE_CHART}
      height={HEIGHT_OF_PIE_CHART}
    />
  );
};

AlgorithmDistribution.propTypes = {
  models: PropTypes.arrayOf(PropTypes.object)
};

const mapStateToAlgorithmDistributionProps = (state) => ({ models: state.models });

const ConnectedAlgorithmDistribution = connect(mapStateToAlgorithmDistributionProps)(AlgorithmDistribution);

const DetailedTopPanel = () => {
  return (
    <TopPanel className="detailed-view">
      <div className="experiment-toppanel-container">
        <ConnectedMetadata />
        <ConnectedAlgorithmDistribution />
      </div>
    </TopPanel>
  );
};

export default DetailedTopPanel;
