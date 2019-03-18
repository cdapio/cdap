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
import AddFeatureActions from '../../../../services/WizardStores/AddFeature/AddFeatureActions';
import AddFeatureStore from '../../../../services/WizardStores/AddFeature/AddFeatureStore';
import PropertySelector from '../../PropertySelector';


const mapStateToConfigurationProps = (state) => {
  return {
    selectedSchemas: state.featureState.selectedSchemas,
    propertyMap: state.featureState.propertyMap,
    availableProperties: state.featureState.availableProperties,
    detectedProperties: state.featureState.detectedProperties,
  };
};

const mapDispatchToConfigurationProps = (dispatch) => {
  return {
    updatePropertyMap: (value) => {
      dispatch({
        type: AddFeatureActions.updatePropertyMap,
        payload: value
      });
    },
    setDetectedProperties: (value) => {
      dispatch({
        type: AddFeatureActions.setDetectedProperties,
        payload: value
      });
    },
  };
};

const FeatureProperties = connect(
  mapStateToConfigurationProps,
  mapDispatchToConfigurationProps
)(PropertySelector);


export default function FeaturePropertiesStep() {
  return (
    <Provider store={AddFeatureStore}>
      <FeatureProperties/>
    </Provider>
  );
}
