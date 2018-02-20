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
import React, { Component } from 'react';
import { Provider } from 'react-redux';
import experimentDetailStore, {DEFAULT_EXPERIMENT_DETAILS} from 'components/Experiments/store/experimentDetailStore';
import {
  getExperimentDetails,
  getModelsInExperiment,
  getModelStatus,
  updatePaginationForModels,
  handleModelsPageChange,
  resetExperimentDetailStore,
  resetNewlyTrainingModel,
  setAlgorithmsList
} from 'components/Experiments/store/ActionCreator';
import ConnectedTopPanel from 'components/Experiments/DetailedView/TopPanel';
import ModelsTableWrapper from 'components/Experiments/DetailedView/ModelsTable';
import Mousetrap from 'mousetrap';
import isNil from 'lodash/isNil';
import queryString from 'query-string';
import Alert from 'components/Alert';

require('./DetailedView.scss');

export default class ExperimentDetails extends Component {
  static propTypes = {
    match: PropTypes.object,
    location: PropTypes.object
  };

  modelStatusObservables = [];

  componentWillMount() {
    Mousetrap.bind('right', this.goToNextPage);
    Mousetrap.bind('left', this.goToPreviousPage);
    setAlgorithmsList();
    let { experimentId } = this.props.match.params;
    let { offset: modelsOffset, limit: modelsLimit } = this.getQueryObject(queryString.parse(this.props.location.search));
    updatePaginationForModels({modelsOffset, modelsLimit});
    getExperimentDetails(experimentId);
    getModelsInExperiment(experimentId).subscribe(({models}) => {
      models.forEach(model => this.modelStatusObservables.push(getModelStatus(experimentId, model.id)));
    });
  }

  componentWillUnmount() {
    Mousetrap.unbind('left');
    Mousetrap.unbind('right');
    resetExperimentDetailStore();
    if (this.modelStatusObservables.length) {
      this.modelStatusObservables.forEach(statusObservable$ => statusObservable$.unsubscribe());
    }
  }

  showNewlyTrainingModel = () => {
    let {newlyTrainingModel} = experimentDetailStore.getState();
    if (newlyTrainingModel) {
      return (
        <Alert
          message={`You have successfully started training the model: ${newlyTrainingModel.name}`}
          type='success'
          showAlert={true}
          onClose={resetNewlyTrainingModel}
        />
      );
    }
  };

  goToNextPage = () => {
    let {modelsOffset, modelsLimit, modelsTotalPages} = experimentDetailStore.getState();
    let nextPage = modelsOffset === 0 ? 1 : Math.ceil((modelsOffset + 1) / modelsLimit);
    if (nextPage < modelsTotalPages) {
      handleModelsPageChange({ selected: nextPage });
    }
  };

  goToPreviousPage = () => {
    let {modelsOffset, modelsLimit} = experimentDetailStore.getState();
    let prevPage = modelsOffset === 0 ? 1 : Math.ceil((modelsOffset + 1) / modelsLimit);
    if (prevPage > 1) {
      handleModelsPageChange({ selected: prevPage - 2 });
    }
  };

  getQueryObject = (query) => {
    if (isNil(query)) {
      return {};
    }
    let {
      offset = DEFAULT_EXPERIMENT_DETAILS.modelsOffset,
      limit = DEFAULT_EXPERIMENT_DETAILS.modelsLimit
    } = query;
    offset = parseInt(offset, 10);
    limit = parseInt(limit, 10);
    if (isNaN(offset)) {
      offset = DEFAULT_EXPERIMENT_DETAILS.modelsOffset;
    }
    if (isNaN(limit)) {
      limit = DEFAULT_EXPERIMENT_DETAILS.modelsLimit;
    }
    return { offset, limit };
  };

  render() {
    return (
      <Provider store={experimentDetailStore}>
        <div className="experiment-detailed-view">
          <ConnectedTopPanel />
          <ModelsTableWrapper />
          {this.showNewlyTrainingModel()}
        </div>
      </Provider>
    );
  }
}
