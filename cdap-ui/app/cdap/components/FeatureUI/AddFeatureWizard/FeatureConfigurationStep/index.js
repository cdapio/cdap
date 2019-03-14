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

/* eslint react/prop-types: 0 */

import React from 'react';
import { Provider, connect } from 'react-redux';
import AddFeatureActions from '../../../../services/WizardStores/AddFeature/AddFeatureActions';
import Configurator from '../../Configurator';
import AddFeatureStore from '../../../../services/WizardStores/AddFeature/AddFeatureStore';


const mapStateToConfigurationProps = (state) => {
  console.log("mapStateToConfigurationProps -> " ,state);
  return {
    availableConfigurations: state.featureState.availableConfigurations,
    configurationList: state.featureState.configurationList
  };
};

const mapDispatchToConfigurationProps = (dispatch) => {
  return {
    updateConfigurationList: (list) => {
      console.log("updateConfigurationList -> " ,list);
      dispatch({
        type: AddFeatureActions.updateConfigurationList,
        payload: list
      });
    }
  };
};

const FeatureConfiguration = connect(
  mapStateToConfigurationProps,
  mapDispatchToConfigurationProps
)(Configurator);


export default function FeatureConfigurationStep() {
  return (
    <Provider store={AddFeatureStore}>
      <FeatureConfiguration/>
    </Provider>
  );
}
