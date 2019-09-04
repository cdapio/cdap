
import React from 'react';
import { Provider, connect } from 'react-redux';
import LandingPage from './LandingPage/index.js';
import MRDSActions from '../../services/ModelManagementStore/actions.js';
import MRDSStore from '../../services/ModelManagementStore/store.js';

const mapStateToFeatureUIProps = (state) => {
  return {
    experiments: state.mrdsState.experiments,
  };
};

const mapDispatchToFeatureUIProps = (dispatch) => {
  return {
    fetchExperiment: (experiments) => {
      dispatch({
        type: MRDSActions.fetchExperiements,
        payload: experiments
      });
    }
  };
};

const MRDSLandingPage = connect(
  mapStateToFeatureUIProps,
  mapDispatchToFeatureUIProps
)(LandingPage);


export default function MRDSUI() {
  return (
    <Provider store={MRDSStore}>
      <MRDSLandingPage/>
    </Provider>
  );
}
