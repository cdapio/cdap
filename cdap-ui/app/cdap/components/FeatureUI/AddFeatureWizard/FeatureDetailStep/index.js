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
import AddFeatureActions from '../../../../services/WizardStores/AddFeature/AddFeatureActions';
import AddFeatureStore from '../../../../services/WizardStores/AddFeature/AddFeatureStore';
import DetailProvider from '../../DetailProvider';


const mapStateToSelectSchemaProps = (state) => {
  return {
    operationType: state.featureState.operationType,
    featureName: state.featureState.featureName,
  };
};

const mapDispatchToSelectSchemaProps = (dispatch) => {
  return {
    updateFeatureName: (featureName) => {
      dispatch({
        type: AddFeatureActions.updateFeatureName,
        payload: featureName
      });
    },
  };
};

const FeatureDetailProvider = connect(
  mapStateToSelectSchemaProps,
  mapDispatchToSelectSchemaProps
)(DetailProvider);


export default function FeatureDetailStep() {
  return (
    <Provider store={AddFeatureStore}>
      <FeatureDetailProvider/>
    </Provider>
  );
}
