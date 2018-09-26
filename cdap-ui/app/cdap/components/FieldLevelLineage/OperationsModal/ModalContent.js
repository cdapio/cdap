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

function formatDatasets(datasets) {
  return datasets
    .map((dataset) => `'${dataset}'`)
    .join('; ');
}

function getDatasets(operations) {
  const inputs = [];
  const outputs = [];

  operations.forEach((operation) => {
    const input = objectQuery(operation, 'inputs', 'endPoint', 'name');
    const output = objectQuery(operation, 'outputs', 'endPoint', 'name');

    if (input) {
      inputs.push(input);
    }

    if (output) {
      outputs.push(output);
    }
  });

  return {
    sources: formatDatasets(inputs),
    targets: formatDatasets(outputs),
  };
}

function ModalContentView({operations, activeIndex}) {
  const activeSet = operations[activeIndex];
  const lastApp = objectQuery(activeSet, 'programs', 0);
  const application = objectQuery(lastApp, 'program', 'application');
  const lastExecutedTime = objectQuery(lastApp, 'lastExecutedTimeInSeconds');
  const activeOperations = activeSet.operations;

  const {
    sources,
    targets
  } = getDatasets(activeOperations);

  return (
    <div className="operations-container">
      <Navigation />

      <div className="summary-text">
        {T.translate(`${PREFIX}.summaryText`, { sources, targets })}
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
};

const mapStateToProps = (state) => {
  return {
    operations: state.operations.operations,
    activeIndex: state.operations.activeIndex,
  };
};

const ModalContent = connect(
  mapStateToProps
)(ModalContentView);

export default ModalContent;
