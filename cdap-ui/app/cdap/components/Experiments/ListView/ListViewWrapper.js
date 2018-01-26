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
import SortableStickyTable from 'components/SortableStickyTable';
import PieChart from 'components/PieChart';
import PaginationWithTitle from 'components/PaginationWithTitle';
import d3 from 'd3';
import ExperimentsListBarChart from 'components/Experiments/ListView/ExperimentsListBarChart';
import PlusButton from 'components/PlusButton';
import InvalidPageView from 'components/Experiments/ListView/InvalidPageView';
import EmptyListView from 'components/Experiments/ListView/EmptyListView';
import NamespaceStore, { getCurrentNamespace } from 'services/NamespaceStore';
import { Link } from 'react-router-dom';
import {handlePageChange} from 'components/Experiments/store/ActionCreator';


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
    label: '#Deployed',
    property: 'numOfDeployedModels'
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

const colorScale = d3.scale.category20();
const PLUSBUTTONCONTEXTMENUITEMS = [
  {
    label: 'Create a new Experiment',
    to: `/ns/${getCurrentNamespace()}/experiments/create`
  }
];

const getAlgoDistribution = (models) => {
  if (!models.length) {
    return null;
  }
  let modelsMap = {};
  models.forEach(model => {
    let algo = model.algorithm;
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

const renderTableBody = (entities) => {
  let list = entities.map(entity => {
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
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return (
    <table className="table">
      <tbody>
        {
          list.map(entity => {
            return (
              <tr>
                <Link to={`/ns/${namespace}/experiments/${entity.name}`}>
                  <td>
                    <h5>
                      <div>{entity.name}</div>
                      <small>{entity.description}</small>
                    </h5>
                  </td>
                  <td>{entity.numOfModels}</td>
                  <td>{entity.numOfDeployedModels}</td>
                  <td>{!entity.algorithmTypes ? null : <PieChart data={entity.algorithmTypes} />}</td>
                  <td>{entity.testData}</td>
                </Link>
              </tr>
            );
          })
        }
      </tbody>
    </table>
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
      },
      {
        name: experiment.name,
        type: 'Deployed',
        count: Array.isArray(experiment.models) ? experiment.models.filter(model => model.deploytime).length : 0
      }
    );
  });
  return data;
};

function ExperimentsListView({ loading, list, totalPages, currentPage, totalCount }) {
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
        <SortableStickyTable
          entities={list}
          tableHeaders={tableHeaders}
          renderTableBody={renderTableBody}
        />
      </div>
    </div>
  );
}

ExperimentsListView.propTypes = {
  loading: PropTypes.bool,
  list: PropTypes.arrayOf(PropTypes.object),
  totalPages: PropTypes.number,
  currentPage: PropTypes.number,
  totalCount: PropTypes.number
};

const mapStateToProps = (state) => {
  return {
    loading: state.experiments.loading,
    list: state.experiments.list,
    totalPages: state.experiments.totalPages,
    currentPage: state.experiments.offset === 0 ? 1 : Math.ceil((state.experiments.offset + 1) / state.experiments.limit),
    totalCount: state.experiments.totalCount
  };
};

const ExperimentsListViewWrapper = connect(mapStateToProps)(ExperimentsListView);

export default ExperimentsListViewWrapper;
