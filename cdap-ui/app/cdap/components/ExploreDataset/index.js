/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import { Provider, connect } from 'react-redux';
import ExploreDatasetPage from './ExploreDatasetPage';
import ExploreDatasetActions from './store/ExploreDatasetActions';
import ExploreDatasetStore from './store/ExploreDatasetStore';

const mapStateToExploreDatasetProps = (state) => {
  return {
    actionType:  state.exploreDatasetState.actionType,
    schema: state.exploreDatasetState.schema,
    availableOperations: state.exploreDatasetState.availableOperations,
    availableEngineConfigurations: state.exploreDatasetState.availableEngineConfigurations,
    availableSinks: state.exploreDatasetState.availableSinks,
    pipelineName: state.exploreDatasetState.pipelineName,
    operationConfigurations: state.exploreDatasetState.operationConfigurations,
    engineConfigurations: state.exploreDatasetState.engineConfigurations,
    sinkConfigurations: state.exploreDatasetState.sinkConfigurations,
    extraConfigurations: state.exploreDatasetState.extraConfigurations,
  };
};

const mapDispatchToExploreDatasetProps = (dispatch) => {
  return {
    resetStore: () => {
      dispatch({
        type: ExploreDatasetActions.onReset,
        payload: undefined
      });
    },
    setAvailableSinks: (configurations) => {
      dispatch({
        type: ExploreDatasetActions.setAvailableSinks,
        payload: configurations
      });
    },
    updatePipelineName: (pipelineName) => {
      dispatch({
        type: ExploreDatasetActions.updatePipelineName,
        payload: pipelineName
      });
    },
    setAvailableOperations: (configurations) => {
      dispatch({
        type: ExploreDatasetActions.setAvailableOperations,
        payload: configurations
      });
    },
    setAvailableEngineConfigurations: (configurations) => {
      dispatch({
        type: ExploreDatasetActions.setAvailableEngineConfigurations,
        payload: configurations
      });
    },
    setSchema: (schema) => {
      dispatch({
        type: ExploreDatasetActions.setSchema,
        payload: schema
      });
    },
    updateEngineConfigurations: (configurations) => {
      dispatch({
        type: ExploreDatasetActions.updateEngineConfigurations,
        payload: configurations
      });
    },
  };
};

const ExploreDatasetUIPage = connect(
    mapStateToExploreDatasetProps,
    mapDispatchToExploreDatasetProps
)(ExploreDatasetPage);


export default function ExploreDatasetUI() {
  return (
    <Provider store={ExploreDatasetStore}>
      <ExploreDatasetUIPage/>
    </Provider>
  );
}
