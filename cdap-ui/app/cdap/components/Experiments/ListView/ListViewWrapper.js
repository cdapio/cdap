/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import TopPanel from 'components/Experiments/TopPanel';
import PieChart from 'components/PieChart';
import PaginationWithTitle from 'components/PaginationWithTitle';
import * as d3Lib from 'd3';
import ExperimentsListBarChart from 'components/Experiments/ListView/ExperimentsListBarChart';
import PlusButton from 'components/PlusButton';
import InvalidPageView from 'components/Experiments/ListView/InvalidPageView';
import EmptyListView from 'components/Experiments/ListView/EmptyListView';
import NamespaceStore, { getCurrentNamespace } from 'services/NamespaceStore';
import { Link } from 'react-router-dom';
import {handlePageChange, handleExperimentsSort, setExperimentsListError} from 'components/Experiments/store/ExperimentsListActionCreator';
import IconSVG from 'components/IconSVG';
import Alert from 'components/Alert';

require('./ListView.scss');

const tableHeaders = [
  {
    label: 'Experiment',
    property: 'name'
  },
  {
    label: '#Models',
    property: 'numOfModels'
  },
  {
    label: 'Algorithm Types',
    property: 'algorithmTypes'
  },
  {
    label: 'Test Data',
    property: 'testData'
  }
];

const colorScale = d3Lib.scaleOrdinal(d3Lib.schemeCategory20);
const PLUSBUTTONCONTEXTMENUITEMS = [
  {
    label: 'Create a new Experiment',
    to: `/ns/${getCurrentNamespace()}/experiments/create`
  }
];

const getAlgoDistribution = (models) => {
  if (!models.length) {
    return [];
  }
  let modelsMap = {};
  models.forEach(model => {
    let algo = model.algorithm;
    if (!algo) {
      return;
    }
    if (!modelsMap[algo]) {
      modelsMap = {
        ...modelsMap,
        [algo]: {
          value: algo,
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

const renderGrid = (experiments, sortMethod, sortColumn) => {
  let list = experiments.map(entity => {
    let models = entity.models || [];
    let modelsCount = entity.modelsCount;
    return {
      name: entity.name,
      description: entity.description,
      numOfModels: modelsCount,
      numOfDeployedModels: models.filter(model => model.deploytime).length,
      testData: entity.srcpath.split('/').pop(),
      algorithmTypes: getAlgoDistribution(models)
    };
  });
  const renderSortIcon = (sortMethod) =>
    sortMethod === 'asc' ? <IconSVG name="icon-caret-down" /> : <IconSVG name="icon-caret-up" />;

  return (
    <div className="grid grid-container">
      <div className="grid-header">
        <div className="grid-row">
          {
            tableHeaders.map((header, i) => {
              if (sortColumn === header.property) {
                return (
                  <strong
                    className="sortable-header"
                    key={i}
                    onClick={handleExperimentsSort.bind(null, header.property)}
                  >
                    <span>{header.label}</span>
                    {renderSortIcon(sortMethod)}
                  </strong>
                );
              }
              return (
                <strong >
                  {header.label}
                </strong>
              );
            })
          }
        </div>
      </div>
      <div className="grid-body">
        {
          list.map(experiment => {
            return (
              <Link
                to={`/ns/${getCurrentNamespace()}/experiments/${experiment.name}`}
                className="grid-row grid-link"
              >
                <div>
                  <h5>
                    <div>{experiment.name}</div>
                    <small>{experiment.description}</small>
                  </h5>
                </div>
                <div>{experiment.numOfModels}</div>
                <div>{!experiment.algorithmTypes.length ? '--' : <PieChart data={experiment.algorithmTypes} />}</div>
                <div>{experiment.testData}</div>
              </Link>
            );
          })
        }
      </div>
    </div>
  );
};

const getDataForGroupedChart = (experiments) => {
  if (!experiments.length) {
    return null;
  }
  let data = [];
  experiments.map(experiment => {
    data.push(
      {
        name: experiment.name,
        type: 'Models',
        count: Array.isArray(experiment.models) ? experiment.models.length : 0
      }
    );
  });
  return data;
};

function ExperimentsListView({...props}) {
  return (
    <div>
      {ExperimentsListViewContent(props)}
      {
        props.error ?
          <Alert
            message={props.error}
            type='error'
            showAlert={true}
            onClose={setExperimentsListError}
          />
        :
          null
      }
    </div>
  );
}

ExperimentsListView.propTypes = {
  error: PropTypes.any
};

function ExperimentsListViewContent({
  loading,
  list,
  totalPages,
  currentPage,
  totalCount,
  sortMethod,
  sortColumn
}) {
  if (loading) {
    return <LoadingSVGCentered />;
  }
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  if (!list.length) {
    return (
      <div className="experiments-listview">
        <TopPanel>
          <h4>Analytics - All Experiments</h4>
          <PlusButton
            mode={PlusButton.MODE.resourcecenter}
            contextItems={PLUSBUTTONCONTEXTMENUITEMS}
          />
        </TopPanel>
        {
          totalPages ? <InvalidPageView namespace={namespace} /> : <EmptyListView namespace={namespace} />
        }

      </div>
    );
  }
  return (
    <div className="experiments-listview">
      <TopPanel>
        <h4>Analytics - All Experiments</h4>
        <PlusButton
          mode={PlusButton.MODE.resourcecenter}
          contextItems={PLUSBUTTONCONTEXTMENUITEMS}
        />
      </TopPanel>
      <ExperimentsListBarChart
        data={getDataForGroupedChart(list)}
      />
      <div className="clearfix">
        <PaginationWithTitle
          handlePageChange={handlePageChange}
          currentPage={currentPage}
          totalPages={totalPages}
          title={totalCount > 1 ? "Experiments" : "Experiment"}
          numberOfEntities={totalCount}
        />
      </div>
      <div className="grid-wrapper">
        { renderGrid(list, sortMethod, sortColumn) }
      </div>
    </div>
  );
}

ExperimentsListViewContent.propTypes = {
  loading: PropTypes.bool,
  list: PropTypes.arrayOf(PropTypes.object),
  totalPages: PropTypes.number,
  currentPage: PropTypes.number,
  totalCount: PropTypes.number,
  sortMethod: PropTypes.string,
  sortColumn: PropTypes.string
};

const mapStateToProps = (state) => {
  return {
    loading: state.experiments.loading,
    list: state.experiments.list,
    totalPages: state.experiments.totalPages,
    currentPage: state.experiments.offset === 0 ? 1 : Math.ceil((state.experiments.offset + 1) / state.experiments.limit),
    totalCount: state.experiments.totalCount,
    sortMethod: state.experiments.sortMethod,
    sortColumn: state.experiments.sortColumn,
    error: state.experiments.error
  };
};

const ExperimentsListViewWrapper = connect(mapStateToProps)(ExperimentsListView);

export default ExperimentsListViewWrapper;
