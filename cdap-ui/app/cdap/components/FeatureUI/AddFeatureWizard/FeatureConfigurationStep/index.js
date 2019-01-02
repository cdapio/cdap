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