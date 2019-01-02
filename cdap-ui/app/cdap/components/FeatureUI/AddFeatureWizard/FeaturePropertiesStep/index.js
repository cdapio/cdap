import React from 'react';
import { Provider, connect } from 'react-redux';
import AddFeatureActions from '../../../../services/WizardStores/AddFeature/AddFeatureActions';
import AddFeatureStore from '../../../../services/WizardStores/AddFeature/AddFeatureStore';
import PropertySelector from '../../PropertySelector';


const mapStateToConfigurationProps = (state) => {
  console.log("mapStateToConfigurationProps -> " ,state);
  return {
    selectedSchemas: state.featureState.selectedSchemas,
    propertyMap: state.featureState.propertyMap,
    availableProperties: state.featureState.availableProperties,
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