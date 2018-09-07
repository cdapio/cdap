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
import {connect} from 'react-redux';
import {humanReadableDate, objectQuery} from 'services/helpers';
import Navigation from 'components/FieldLevelLineage/OperationsModal/Navigation';
import OperationsTable from 'components/FieldLevelLineage/OperationsModal/OperationsTable';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.OperationsModal';

function getInputDatasets(operations) {
  let inputDatasets = [];

  operations.forEach((operation) => {
    let datasetName = objectQuery(operation, 'inputs', 'endPoint', 'name');

    if (datasetName) {
      inputDatasets.push(datasetName);
    }
  });

  return inputDatasets
    .map((dataset) => `'${dataset}'`)
    .join('; ');
}

function ModalContentView({operations, activeIndex, datasetId}) {
  const activeSet = operations[activeIndex];
  const lastApp = objectQuery(activeSet, 'programs', 0);
  const application = objectQuery(lastApp, 'program', 'application');
  const lastExecutedTime = objectQuery(lastApp, 'lastExecutedTimeInSeconds');
  const activeOperations = activeSet.operations;

  return (
    <div className="operations-container">
      <Navigation />

      <div className="summary-text">
        {T.translate(`${PREFIX}.summaryText`, { sources: getInputDatasets(activeOperations), target: datasetId })}
      </div>

      <div className="last-execution">
        {T.translate(`${PREFIX}.lastExecution`, { app: application, time: humanReadableDate(lastExecutedTime) })}
      </div>

      <OperationsTable operations={activeOperations} />
    </div>
  );
}

ModalContentView.propTypes = {
  operations: PropTypes.array,
  activeIndex: PropTypes.number,
  datasetId: PropTypes.string
};

const mapStateToProps = (state) => {
  return {
    operations: state.operations.operations,
    activeIndex: state.operations.activeIndex,
    datasetId: state.lineage.datasetId
  };
};

const ModalContent = connect(
  mapStateToProps
)(ModalContentView);

export default ModalContent;
