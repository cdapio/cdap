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
  pollModelStatus,
  updateQueryParametersForModels,
  handleModelsPageChange,
  resetExperimentDetailStore,
  resetNewlyTrainingModel,
  setAlgorithmsListForDetailedView
} from 'components/Experiments/store/ExperimentDetailActionCreator';
import {MMDS_SORT_METHODS, MMDS_SORT_COLUMN} from 'components/Experiments/store';
import ConnectedTopPanel from 'components/Experiments/DetailedView/TopPanel';
import ModelsTableWrapper from 'components/Experiments/DetailedView/ModelsTable';
import Mousetrap from 'mousetrap';
import isNil from 'lodash/isNil';
import queryString from 'query-string';
import Alert from 'components/Alert';
import Page404 from 'components/404';
import T from 'i18n-react';
import {Link} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';

require('./DetailedView.scss');

export default class ExperimentDetails extends Component {
  static propTypes = {
    match: PropTypes.object,
    location: PropTypes.object
  };

  state = {
    pageNotFound: false
  };

  modelStatusObservables = [];

  componentWillMount() {
    Mousetrap.bind('right', this.goToNextPage);
    Mousetrap.bind('left', this.goToPreviousPage);
    setAlgorithmsListForDetailedView();
    let { experimentId } = this.props.match.params;
    let {
      offset: modelsOffset,
      limit: modelsLimit,
      sortMethod: modelsSortMethod,
      sortColumn: modelsSortColumn
    } = this.getQueryObject(queryString.parse(this.props.location.search));
    updateQueryParametersForModels({
      modelsOffset,
      modelsLimit,
      modelsSortMethod,
      modelsSortColumn
    });
    getExperimentDetails(experimentId);
    getModelsInExperiment(experimentId).subscribe(({models}) => {
      models.forEach(model => this.modelStatusObservables.push(pollModelStatus(experimentId, model.id)));
    });
  }

  componentDidMount() {
    this.experimentDetailStoreSubscription = experimentDetailStore.subscribe(() => {
      let {error} = experimentDetailStore.getState();
      if (error === 404) {
        this.setState({
          pageNotFound: true
        });
      }
    });
  }
  componentWillUnmount() {
    Mousetrap.unbind('left');
    Mousetrap.unbind('right');
    resetExperimentDetailStore();
    if (this.modelStatusObservables.length) {
      this.modelStatusObservables.forEach(statusObservable$ => statusObservable$.unsubscribe());
    }
    if (this.experimentDetailStoreSubscription) {
      this.experimentDetailStoreSubscription();
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
      limit = DEFAULT_EXPERIMENT_DETAILS.modelsLimit,
      sort
    } = query;
    let sortMethod, sortColumn;
    offset = parseInt(offset, 10);
    limit = parseInt(limit, 10);
    if (isNaN(offset)) {
      offset = DEFAULT_EXPERIMENT_DETAILS.modelsOffset;
    }
    if (isNaN(limit)) {
      limit = DEFAULT_EXPERIMENT_DETAILS.modelsLimit;
    }
    if (!sort) {
      sortMethod = MMDS_SORT_METHODS.ASC;
      sortColumn = MMDS_SORT_COLUMN;
    } else {
      let sortSplit = sort.split(' ');
      sortColumn = sortSplit[0] || MMDS_SORT_COLUMN;
      sortMethod = sortSplit[1] || MMDS_SORT_METHODS.ASC;
    }
    return { offset, limit, sortMethod, sortColumn };
  };

  render() {
    let {pageNotFound} = this.state;
    if (pageNotFound) {
      let namespace = getCurrentNamespace();
      return (
        <Page404
          entityType="experiment"
          entityName={this.props.match.params.experimentId}
        >
          <div className="message-section">
            <h4>
              <strong>
                {T.translate('features.Page404.subtitleMessage1')}
              </strong>
            </h4>
            <div className="navigation-section">
              <div>
                {T.translate('features.Page404.manageLabel')}
                <Link to={`/ns/${namespace}/experiments`}>
                  experiments
                </Link>
              </div>
            </div>
          </div>
        </Page404>
      );
    }
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
