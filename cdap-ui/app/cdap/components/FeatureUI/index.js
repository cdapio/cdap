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
import LandingPage from './LandingPage';
import AddFeatureActions from '../../services/WizardStores/AddFeature/AddFeatureActions';
import AddFeatureStore from '../../services/WizardStores/AddFeature/AddFeatureStore';


const mapStateToFeatureUIProps = (state) => {
  return {
    availableSchemas: state.featureState.availableSchemas,
    selectedSchemas: state.featureState.selectedSchemas,
    propertyMap: state.featureState.propertyMap,
    configurationList: state.featureState.configurationList,
    featureName: state.featureState.featureName,
  };
};

const mapDispatchToFeatureUIProps = (dispatch) => {
  return {
    resetStore: () => {
      dispatch({
        type: AddFeatureActions.onReset,
        payload: undefined
      });
    },
    updateFeatureName: (featureName) => {
      dispatch({
        type: AddFeatureActions.updateFeatureName,
        payload: featureName
      });
    },
    setAvailableSchemas: (schemas) => {
      dispatch({
        type: AddFeatureActions.setAvailableSchemas,
        payload: schemas
      });
    },
    setAvailableProperties: (properties) => {
      dispatch({
        type: AddFeatureActions.setAvailableProperties,
        payload: properties
      });
    },
    setAvailableConfigurations: (configurations) => {
      dispatch({
        type: AddFeatureActions.setAvailableConfigurations,
        payload: configurations
      });
    },
    setAvailableConfigurations: (configurations) => {
      dispatch({
        type: AddFeatureActions.setAvailableConfigurations,
        payload: configurations
      });
    },
    setSelectedSchemas: (value) => {
      dispatch({
        type: AddFeatureActions.setSelectedSchemas,
        payload: value
      });
    },
    updatePropertyMap: (value) => {
      dispatch({
        type: AddFeatureActions.updatePropertyMap,
        payload: value
      });
    },
    updateConfigurationList: (list) => {
      console.log("updateConfigurationList -> " ,list);
      dispatch({
        type: AddFeatureActions.updateConfigurationList,
        payload: list
      });
    }
  };
};

const FeatureUIPage = connect(
  mapStateToFeatureUIProps,
  mapDispatchToFeatureUIProps
)(LandingPage);


export default function FeatureUI() {
  return (
    <Provider store={AddFeatureStore}>
      <FeatureUIPage/>
    </Provider>
  );
}