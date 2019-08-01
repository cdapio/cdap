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
import React from 'react';
import { Provider, connect } from 'react-redux';
import ExploreDatasetStore from 'components/ExploreDataset/store/ExploreDatasetStore';
import EngineConfigurator from 'components/ExploreDataset/Common/EngineConfigurator';
import ExploreDatasetActions from 'components/ExploreDataset/store/ExploreDatasetActions';

const mapStateToConfigurationProps = (state) => {
  return {
    engineConfigurations: state.exploreDatasetState.engineConfigurations
  };
};

const mapDispatchToConfigurationProps = (dispatch) => {
  return {
    updateEngineConfigurations: (list) => {
      dispatch({
        type: ExploreDatasetActions.updateEngineConfigurations,
        payload: list
      });
    }
  };
};

const DatasetEngineConfiguration = connect(
  mapStateToConfigurationProps,
  mapDispatchToConfigurationProps
)(EngineConfigurator);


export default function DatasetEngineConfigurationStep() {
  return (
    <Provider store={ExploreDatasetStore}>
      <DatasetEngineConfiguration/>
    </Provider>
  );
}
