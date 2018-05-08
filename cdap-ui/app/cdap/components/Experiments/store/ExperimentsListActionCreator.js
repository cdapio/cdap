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

import experimentsStore, {ACTIONS, MMDS_SORT_METHODS} from 'components/Experiments/store';
import {setAlgorithmsList} from 'components/Experiments/store/SharedActionCreator';
import {myExperimentsApi} from 'api/experiments';
import {getCurrentNamespace} from 'services/NamespaceStore';

function setExperimentsLoading() {
  experimentsStore.dispatch({
    type: ACTIONS.SET_EXPERIMENTS_LOADING
  });
}

function setExperimentsListError(error = null) {
  experimentsStore.dispatch({
    type: ACTIONS.SET_ERROR,
    payload: {
      error
    }
  });
}

function getExperimentsList() {
  setExperimentsLoading();
  let {
    offset,
    limit,
    sortMethod,
    sortColumn
  } = experimentsStore.getState().experiments;
  myExperimentsApi
    .list({
      namespace: getCurrentNamespace(),
      offset,
      limit,
      sort: `${sortColumn} ${sortMethod}`
    })
    .subscribe(res => {
      let experiments = res.experiments;
      let totalCount = res.totalRowCount;
      experiments.forEach(experiment => getModelsListInExperiment(experiment.name));
      experimentsStore.dispatch({
        type: ACTIONS.SET_EXPERIMENTS_LIST,
        payload: {
          experiments,
          totalCount
        }
      });
    }, (err) => {
      setExperimentsListError(`Failed to get experiments: ${err.response || err}`);
    });
}

function getModelsListInExperiment(experimentId) {
  myExperimentsApi
    .getModelsInExperiment({
      experimentId,
      namespace: getCurrentNamespace()
    })
    .subscribe(res => {
      let models = res.models;
      let modelsCount = res.totalRowCount;
      experimentsStore.dispatch({
        type: ACTIONS.SET_MODELS_IN_EXPERIMENT,
        payload: {
          experimentId,
          models,
          modelsCount
        }
      });
    }, (err) => {
      setExperimentsListError(`Failed to get model count for experiment '${experimentId}': ${err.response || err}`);
    });
}

function handlePageChange({selected}) {
  let {limit} = experimentsStore.getState().experiments;
  experimentsStore.dispatch({
    type: ACTIONS.SET_PAGINATION,
    payload: {
      offset: selected * limit
    }
  });
  updateQueryString();
  getExperimentsList();
}

function handleExperimentsSort(field) {
  let { sortColumn, sortMethod } = experimentsStore.getState().experiments;
  let newSortField = (field !== sortColumn) ? field : sortColumn;
  let newSortMethod = MMDS_SORT_METHODS.ASC === sortMethod ? MMDS_SORT_METHODS.DESC : MMDS_SORT_METHODS.ASC;
  experimentsStore.dispatch({
    type: ACTIONS.SET_EXPERIMENTS_SORT,
    payload: {
      sortMethod: newSortMethod,
      sortColumn: newSortField
    }
  });
  updateQueryString();
  getExperimentsList();
}

const setAlgorithmsListForListView = () => {
  setAlgorithmsList()
    .subscribe(
      () => {},
      (err) => {
        setExperimentsListError(`Failed to get list of algorithms: ${err.response || err}`);
      }
    );
};

function updateQueryParameters({ limit, offset, sortMethod, sortColumn }) {
  experimentsStore.dispatch({
    type: ACTIONS.SET_QUERY_PARAMS,
    payload: {
      limit,
      offset,
      sortMethod,
      sortColumn
    }
  });
}

function updateQueryString() {
  let {
    offset,
    limit,
    sortMethod,
    sortColumn
  } = experimentsStore.getState().experiments;
  let newQuery = `offset=${offset}&limit=${limit}&sort=${sortColumn} ${sortMethod}`;
  let obj = {
    title: document.title,
    url: `${location.pathname}?${newQuery}`
  };
  history.pushState(obj, obj.title, obj.url);
}

export {
  setExperimentsListError,
  getExperimentsList,
  getModelsListInExperiment,
  handlePageChange,
  handleExperimentsSort,
  setAlgorithmsListForListView,
  updateQueryParameters
};

