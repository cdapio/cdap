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

import React, {PropTypes} from 'react';
import { connect, Provider } from 'react-redux';
import OneStepDeployStore from 'services/WizardStores/OneStepDeploy/OneStepDeployStore';
import T from 'i18n-react';
require('./Deploy.scss');

const mapStateWithProps = (state) => {
  return {
    name: state.oneStepDeploy.name
  };
};

let ApplicationName = ({name}) => {
  return (
    <h3 className="text-xs-center">
      {name}
    </h3>
  );
};

ApplicationName.propTypes = {
  name: PropTypes.string
};

ApplicationName = connect(
  mapStateWithProps,
  null
)(ApplicationName);


export default function Deploy() {
  return (
    <Provider store={OneStepDeployStore}>
      <div className="deploy-step">
        <h3 className="text-xs-center">
          {T.translate('features.Wizard.OneStepDeploy.Step1.content')}
        </h3>
        <ApplicationName />
      </div>
    </Provider>
  );
}
