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
