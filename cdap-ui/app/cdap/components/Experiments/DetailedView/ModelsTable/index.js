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
import SortableStickyGrid from 'components/SortableStickyGrid';
import PaginationWithTitle from 'components/PaginationWithTitle';
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';
import {setActiveModel, getAlgorithmLabel} from 'components/Experiments/store/ActionCreator';
import {humanReadableDate} from 'services/helpers';
import {NUMBER_TYPES} from 'services/global-constants';
import classnames from 'classnames';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {objectQuery} from 'services/helpers';
import isEmpty from 'lodash/isEmpty';
import ModelStatusIndicator from 'components/Experiments/DetailedView/ModelStatusIndicator';
import {Link} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import DeleteModelBtn from 'components/Experiments/DetailedView/DeleteModelBtn';
import DeleteExperimentBtn from 'components/Experiments/DetailedView/DeleteExperimentBtn';

require('./DetailedViewModelsTable.scss');

let tableHeaders = [
  {
    label: '',
    width: '2%'
  },
  {
    label: 'Model Name',
    property: 'name',
    width: '20%'
  },
  {
    label: 'Status',
    property: 'status',
    width: '10%'
  },
  {
    label: 'Algorithm',
    property: 'algorithm',
    width: '15%'
  },
  {
    label: '',
    width: '2%'
  },
];

const regressionMetrics = [
  {
    label: 'rmse',
    property: 'rmse',
    width: '12%'
  },
  {
    label: 'r2',
    property: 'r2',
    width: '13%'
  },
  {
    label: 'evariance',
    property: 'evariance',
    width: '13%'
  },
  {
    label: 'mae',
    property: 'mae',
    width: '13%'
  },
];

const categoricalMetrics = [
  {
    label: 'Precision',
    property: 'precision',
    width: '17%'
  },
  {
    label: 'Recall',
    property: 'recall',
    width: '17%'
  },
  {
    label: 'F1',
    property: 'f1',
    width: '17%'
  },
];

const addMetricsToHeaders = (tableHeaders, metrics) => ([
  ...tableHeaders.slice(0, tableHeaders.length - 1),
  ...metrics,
  ...tableHeaders.slice(tableHeaders.length - 1)
]);

const getNewHeadersBasedOnOutcome = (outcomeType) => (
  NUMBER_TYPES.indexOf(outcomeType) !== -1 ?
    addMetricsToHeaders(tableHeaders, regressionMetrics)
  :
    addMetricsToHeaders(tableHeaders, categoricalMetrics)
);

const renderTableHeaders = (outcomeType, renderSortableTableHeader) => {
  let newHeaders = getNewHeadersBasedOnOutcome(outcomeType);
  return (
    <div className="grid-header">
      {
        newHeaders.map((tableHeader, i) => {
          return (
            <div
              className="grid-header-item"
              title={tableHeader.label}
              key={i}
              style={{ width: `${tableHeader.width}` }}
            >
              {
                tableHeader.property ?
                  renderSortableTableHeader(tableHeader)
                :
                  tableHeader.label
              }
            </div>
          );
        })
      }
    </div>
  );
};

const renderTableBody = (experimentId, outcomeType, models) => {
  let list = models.map(model => {
    let {name, algorithm, hyperparameters} = model;
    return {
      ...model,
      name,
      algorithm: getAlgorithmLabel(algorithm),
      hyperparameters
    };
  });
  const renderItem = (width, content) => (
    <div
      className="grid-body-item"
      title={content}
      style={{ width: `${width}` }}
    >
      {content}
    </div>
  );
  const renderMetrics = (newHeaders, model) => {
    let metrics;
    let len = newHeaders.length - 1;
    let commonHeadersLen = tableHeaders.length - 1;
    if (NUMBER_TYPES.indexOf(outcomeType) !== -1) {
      metrics = newHeaders.slice(commonHeadersLen, len);
    } else {
      metrics = newHeaders.slice(commonHeadersLen, len);
    }
    return metrics.map(t => renderItem(t.width, model.evaluationMetrics[t.property] || '--'));
  };

  let newHeaders = getNewHeadersBasedOnOutcome(outcomeType);
  return (
    <div className="grid-body">
      {
        list.map((model) => {
          return (
            <div
              className={classnames("grid-body-row-container", {
                "opened": model.active
              })}
              key={model.id}
            >
              <div
                className={classnames("grid-body-row", {
                  "opened": model.active
                })}
                onClick={setActiveModel.bind(null, model.id)}
              >
                {renderItem(newHeaders[0].width, <IconSVG name={model.active ? "icon-caret-down" : "icon-caret-right"} />)}
                {renderItem(newHeaders[1].width, model.name)}
                {renderItem(newHeaders[2].width, <ModelStatusIndicator status={model.status || '--'} />)}
                {renderItem(newHeaders[3].width, (
                  <span className="algorithm-cell">
                    <IconSVG name="icon-cog" />
                    <span>{model.algorithm}</span>
                  </span>
                ))}
                {renderMetrics(newHeaders, model)}
                {
                  renderItem(
                    newHeaders[newHeaders.length - 1].width,
                    <DeleteModelBtn
                      experimentId={experimentId}
                      model={model}
                    />
                  )
                }
              </div>
              {
                model.active ?
                  <div className="grid-body-row-details">
                    <div style={{width: tableHeaders[0].width}}></div>
                    <div style={{width: tableHeaders[1].width}}>
                      <div>
                        <strong>Model Description</strong>
                        <div>{model.description}</div>
                      </div>
                      <div>
                        <strong># Directives </strong>
                        <div>{Array.isArray(objectQuery(model, 'splitDetails', 'directives')) ? model.splitDetails.directives.length : '--'}</div>
                      </div>
                      <div>
                        <strong>Features ({model.features.length}) </strong>
                        <div>{model.features.join(',')}</div>
                      </div>
                    </div>
                    <div style={{width: tableHeaders[2].width}}>
                      <div>
                        <strong>Deployed on</strong>
                        <div>{model.deploytime === -1 ? '--' : humanReadableDate(model.deploytime)}</div>
                      </div>
                      <div>
                        <strong> Created on</strong>
                        <div>{humanReadableDate(model.createtime, true)}</div>
                      </div>
                    </div>
                  </div>
                :
                  null
              }
            </div>
          );
       })
      }
    </div>
  );
};

function ModelsTable({experimentId, list, loading, outcomeType}) {
  if (loading || isEmpty(experimentId)) {
    return (
      <LoadingSVGCentered />
    );
  }
  return (
    <div className="experiment-models-table">
      <div className="experiment-table-header">
        <div className="btn-container">
          <Link
            className="btn btn-secondary"
            to={`/ns/${getCurrentNamespace()}/experiments/create?experimentId=${experimentId}`}
          >
            Add a Model
          </Link>
          <DeleteExperimentBtn experimentId={experimentId} />
        </div>
        <PaginationWithTitle
          handlePageChange={(currentPage) => console.log(`Pagination coming soon. Right now in page # ${currentPage}`)}
          currentPage={1}
          totalPages={1}
          title={list.length > 1 ? "Models" : "Model"}
          numberOfEntities={list.length}
        />
      </div>
      <SortableStickyGrid
        entities={list}
        tableHeaders={tableHeaders}
        renderTableHeaders={renderTableHeaders.bind(null, outcomeType)}
        renderTableBody={renderTableBody.bind(null, experimentId, outcomeType)}
      />
    </div>
  );
}

ModelsTable.propTypes = {
  list: PropTypes.array,
  loading: PropTypes.bool,
  experimentId: PropTypes.string,
  outcomeType: PropTypes.string
};

const mapStateToProps = (state) => ({
  list: state.models,
  experimentId: state.name,
  loading: state.loading,
  outcomeType: state.outcomeType
});

const ModelsTableWrapper = connect(mapStateToProps)(ModelsTable);

export default ModelsTableWrapper;
