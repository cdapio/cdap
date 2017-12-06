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
import {setActiveModel, getAlgorithmLabel, getModelsInExperiment} from 'components/Experiments/store/ActionCreator';
import {humanReadableDate} from 'services/helpers';
import DeleteEntityBtn from 'components/DeleteEntityBtn';
import classnames from 'classnames';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { myExperimentsApi } from 'api/experiments';
import {objectQuery} from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
import isEmpty from 'lodash/isEmpty';
import ModelStatusIndicator from 'components/Experiments/DetailedView/ModelStatusIndicator';
import {Link} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';

require('./DetailedViewModelsTable.scss');

const tableHeaders = [
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
    label: 'Hyperparameters',
    property: 'hyperparams',
    width: '10%'
  },
  {
    label: 'Precision',
    property: 'precision',
    width: '10%'
  },
  {
    label: 'Recall',
    property: 'recall',
    width: '10%'
  },
  {
    label: 'F1',
    property: 'f1',
    width: '8%'
  },
  {
    label: 'Accuracy',
    property: 'accuracy',
    width: '8%'
  },
  {
    label: 'RMS',
    property: 'rms',
    width: '5%'
  },
  {
    label: '',
    width: '2%'
  },
];
const renderTableHeaders = (renderSortableTableHeader) => {
  return (
    <div className="grid-header">
      {
        tableHeaders.map(tableHeader => {
          return (
            <div
              className="grid-header-item"
              title={tableHeader.label}
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

const deleteModel = (experimentId, modelId, callback, errCallback) => {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .deleteModelInExperiment({
      namespace,
      experimentId,
      modelId
    })
    .subscribe(
      () => {
        getModelsInExperiment(experimentId);
        callback();
      },
      err => {
        let error = typeof err.response === 'string' ? err.response : JSON.stringify(err);
        errCallback(error);
      }
    );
};

const deleteExperiment = (experimentId, callback, errCallback) => {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .deleteExperiment({
      namespace,
      experimentId
    })
    .subscribe(
      () => window.location.href =`${window.location.origin}/cdap/ns/${namespace}/experiments`,
      err => {
        let error = typeof err.response === 'string' ? err.response : JSON.stringify(err);
        errCallback(error);
      }
    );
};

const renderTableBody = (experimentId, models) => {
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
      title={typeof content === 'string' ? content : null}
      style={{ width: `${width}` }}
    >
      {content}
    </div>
  );
  const deleteConfimElement = (model) => <div>Are you sure you want to delete <b>{model.name}</b> model </div>;
  return (
    <div className="grid-body">
      {
        list.map((model) => {
          return (
            <div className="grid-body-row-container" key={model.id}>
              <div
                className={classnames("grid-body-row", {
                  "opened": model.active
                })}
                onClick={setActiveModel.bind(null, model.id)}
              >
                {renderItem(tableHeaders[0].width, <IconSVG name={model.active ? "icon-caret-down" : "icon-caret-right"} />)}
                {renderItem(tableHeaders[1].width, model.name)}
                {renderItem(tableHeaders[2].width, <ModelStatusIndicator status={model.status} />)}
                {renderItem(tableHeaders[3].width, model.algorithm)}
                {renderItem(tableHeaders[4].width, <IconSVG name="icon-cog" />)}
                {renderItem(tableHeaders[5].width, '--')}
                {renderItem(tableHeaders[6].width, '--')}
                {renderItem(tableHeaders[7].width, '--')}
                {renderItem(tableHeaders[8].width, '--')}
                {renderItem(tableHeaders[9].width, '--')}
                {
                  renderItem(
                    tableHeaders[10].width,
                    <DeleteEntityBtn
                      confirmFn={deleteModel.bind(null, experimentId, model.id)}
                      headerTitle={"Delete Model"}
                      confirmationElem={deleteConfimElement(model)}
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

function ModelsTable({experimentId, list, loading}) {
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
          <DeleteEntityBtn
            confirmFn={deleteExperiment.bind(null, experimentId)}
            className="btn btn-link"
            headerTitle={"Delete Model"}
            confirmationElem={
              <div>Are you sure you want to delete <b>{experimentId}</b> experiment </div>
            }
            btnLabel={"Delete Experiment"}
          />
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
        renderTableHeaders={renderTableHeaders}
        renderTableBody={renderTableBody.bind(null, experimentId)}
      />
    </div>
  );
}

ModelsTable.propTypes = {
  list: PropTypes.arrayOf,
  loading: PropTypes.bool,
  experimentId: PropTypes.string
};

const mapStateToProps = (state) => ({
  list: state.models,
  experimentId: state.name,
  loading: state.loading
});

const ModelsTableWrapper = connect(mapStateToProps)(ModelsTable);

export default ModelsTableWrapper;
