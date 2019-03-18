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
import SchemaSelector from '../../SchemaSelector';
import AddFeatureStore from '../../../../services/WizardStores/AddFeature/AddFeatureStore';


const mapStateToSelectSchemaProps = (state) => {
  return {
    availableSchemas: state.featureState.availableSchemas,
    selectedSchemas: state.featureState.selectedSchemas,
    propertyMap: state.featureState.propertyMap,
    detectedProperties: state.featureState.detectedProperties,
  };
};

const mapDispatchToSelectSchemaProps = (dispatch) => {
  return {
    setAvailableSchemas: (schemas) => {
      dispatch({
        type: AddFeatureActions.setAvailableSchemas,
        payload: schemas
      });
    },
    addSelectedSchema: (schema) => {
      dispatch({
        type: AddFeatureActions.addSelectedSchema,
        payload: schema
      });
    },
    updateSelectedSchema: (schema) => {
      dispatch({
        type: AddFeatureActions.updateSelectedSchema,
        payload: schema
      });
    },
    setSelectedSchemas: (schemas) => {
      dispatch({
        type: AddFeatureActions.setSelectedSchemas,
        payload: schemas
      });
    },
    updatePropertyMap: (schema) => {
      dispatch({
        type: AddFeatureActions.updatePropertyMap,
        payload: schema
      });
    },
    deleteSelectedSchema: (schema) => {
      dispatch({
        type: AddFeatureActions.deleteSelectedSchema,
        payload: schema
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

const FeatureSchemaSelector = connect(
  mapStateToSelectSchemaProps,
  mapDispatchToSelectSchemaProps
)(SchemaSelector);


export default function FeatureSchemaStep() {
  return (
    <Provider store={AddFeatureStore}>
      <FeatureSchemaSelector/>
    </Provider>
  );
}
