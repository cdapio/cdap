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
import ExploreDatasetActions from 'components/ExploreDataset/store/ExploreDatasetActions';
import DetailProvider from 'components/ExploreDataset/Common/DetailProvider';
import ExploreDatasetStore from 'components/ExploreDataset/store/ExploreDatasetStore';


const mapStateToProps = (state) => {
  return {
    operationType: state.exploreDatasetState.operationType,
    pipelineName: state.exploreDatasetState.pipelineName,
    extraConfigurations: state.exploreDatasetState.extraConfigurations,
  };
};

const mapDispatchToProps = (dispatch) => {
  return {
    updatePipelineName: (pipelineName) => {
      dispatch({
        type: ExploreDatasetActions.updatePipelineName,
        payload: pipelineName
      });
    },
    setExtraConfigurations: (extraConfigurations) => {
      dispatch({
        type: ExploreDatasetActions.setExtraConfigurations,
        payload: extraConfigurations
      });
    },
  };
};

const DatasetDetailProvider = connect(
  mapStateToProps,
  mapDispatchToProps
)(DetailProvider);


export default function DatasetDetailStep() {
  return (
    <Provider store={ExploreDatasetStore}>
      <DatasetDetailProvider/>
    </Provider>
  );
}
