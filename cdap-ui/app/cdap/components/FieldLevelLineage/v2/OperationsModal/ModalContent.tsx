/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useContext } from 'react';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';
import { humanReadableDate, objectQuery } from 'services/helpers';
import Navigation from 'components/FieldLevelLineage/v2/OperationsModal/Navigation';
import OperationsTable from 'components/FieldLevelLineage/v2/OperationsModal/OperationsTable';
import Heading, { HeadingTypes } from 'components/Heading';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.OperationsModal';

function formatDatasets(datasets) {
  // return in form 'dataset1, dataset2, and dataset3'
  switch (datasets.length) {
    case 0:
      return '';
    case 1:
      return `'${datasets[0]}'`;
    case 2:
      return `'${datasets[0]}' and ${datasets[1]}`;

    default: {
      const last = datasets[-1];
      const rest = datasets.slice(0, -1);
      return `${rest.map((dataset) => `'${dataset}'`).join(', ')}, and '${last}'`;
    }
  }
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

const ModalContentView: React.FC = () => {
  const { operations, activeOpsIndex } = useContext<IContextState>(FllContext);
  const activeSet = operations[activeOpsIndex];
  const lastApp = objectQuery(activeSet, 'programs', 0);
  const application = objectQuery(lastApp, 'program', 'application');
  const lastExecutedTime = objectQuery(lastApp, 'lastExecutedTimeInSeconds');
  const activeOperations = activeSet.operations;

  const { sources, targets } = getDatasets(activeOperations);

  return (
    <div className="operations-container">
      <Heading
        type={HeadingTypes.h5}
        label={T.translate(`${PREFIX}.summaryText`, { sources, targets })}
        className="summary-text"
      />

      <Navigation />

      <Heading
        type={HeadingTypes.h6}
        label={T.translate(`${PREFIX}.lastExecution`, {
          app: application,
          time: humanReadableDate(lastExecutedTime),
        })}
        className="last-executed"
      />

      <OperationsTable />
    </div>
  );
};

export default ModalContentView;
