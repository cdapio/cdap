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

import React, { Component, PropTypes } from 'react';
import OneStepDeployAppConfig from 'services/WizardConfigs/OneStepDeployAppConfig';
import OneStepDeployPluginConfig from 'services/WizardConfigs/OneStepDeployPluginConfig';
import OneStepDeployStore from 'services/WizardStores/OneStepDeploy/OneStepDeployStore';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import T from 'i18n-react';
import LicenseStep from 'components/CaskWizards/LicenseStep';

export default class OneStepDeployWizard extends Component {
  constructor(props) {
    super(props);

    this.state = {
      showWizard: this.props.isOpen,
      license: this.props.input.package.license ? true : false
    };

    this.publishApp = this.publishApp.bind(this);
    this.showWizardContents = this.showWizardContents.bind(this);
    this.toggleWizard = this.toggleWizard.bind(this);
  }

  showWizardContents() {
    this.setState({
      license: false
    });
  }

  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }

  publishApp() {
    return this.props.onPublish().map(successInfo => {
      this.setState({
        successInfo
      });
    });
  }

  render() {
    let input = this.props.input;
    let pkg = input.package || {};
    let actionType = input.action.type;

    const getWizardContent = () => {
      return (
        <Wizard
          wizardConfig={actionType === 'one_step_deploy_app' ? OneStepDeployAppConfig : OneStepDeployPluginConfig}
          wizardType="OneStepDeploy"
          store={OneStepDeployStore}
          onSubmit={this.publishApp.bind(this)}
          successInfo={this.state.successInfo}
          onClose={this.toggleWizard.bind(this)}
        />
      );
    };

    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.OneStepDeploy.headerlabel');
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="one-step-deploy-wizard"
      >
        {
          this.state.license ?
            <LicenseStep
              entityName={this.props.input.package.name}
              entityVersion={this.props.input.package.version}
              licenseFileName={this.props.input.package.license}
              onAgree={this.showWizardContents}
              onReject={this.toggleWizard.bind(this, false)}
            />
          :
            getWizardContent()
        }
      </WizardModal>
    );
  }
}

OneStepDeployWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  onPublish: PropTypes.func
};
