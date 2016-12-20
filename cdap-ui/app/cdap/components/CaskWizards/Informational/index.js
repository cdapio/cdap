/*
 * Copyright © 2016 Cask Data, Inc.
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
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import InformationalWizardConfig from 'services/WizardConfigs/InformationalWizardConfig';
import InformationalWizardStore from 'services/WizardStores/Informational/InformationalStore';
import InformationalActions from 'services/WizardStores/Informational/InformationalActions';
import {MyMarketApi} from 'api/market';
require('./Informational.less');

import T from 'i18n-react';

export default class InformationalWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      license: this.props.input.package.license ? true : false,
      licenseAgreement: false
    };
    this.prepareInputForSteps();
  }
  componentDidMount() {
    if (this.props.input.package.license) {
      let params = {
        entityName: this.props.input.package.name,
        entityVersion: this.props.input.package.version,
        filename: this.props.input.package.license
      };
      MyMarketApi
        .getSampleData(params)
        .subscribe(license => {
          this.setState({
            license
          });
        });
    }
  }
  prepareInputForSteps() {
    let actionSteps = this.props.input.action.arguments[0].value;

    InformationalWizardStore.dispatch({
      type: InformationalActions.setSteps,
      payload: actionSteps
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
  componentWillUnmount() {
    InformationalWizardStore.dispatch({
      type: InformationalActions.onReset
    });
  }


  showWizard() {
    this.setState({
      licenseAgreement: true
    });
  }
  render() {
    let input = this.props.input;
    let pkg = input.package || {};

    const getWizardContent = () => {
      return (
        <Wizard
          wizardConfig={InformationalWizardConfig}
          store={InformationalWizardStore}
          onSubmit={this.toggleWizard.bind(this, true)}
          onClose={this.toggleWizard.bind(this)}
        />
      );
    };

    const getLicenseTemplate = () => {
      if (typeof this.state.license === 'boolean') {
        return (
          <div className="fa fa-spin fa-spinner"></div>
        );
      }
      if (this.state.licenseAgreement) {
        return getWizardContent();
      } else {
        return (
          <div className="license-container">
            <h2>{T.translate('features.Wizard.Informational.termsandconditions')}</h2>
            <div className="license-text">{this.state.license}</div>
            <div>
              <div
                className="btn btn-primary agree-btn"
                onClick={this.showWizard.bind(this)}
              >
                Agree and Download
              </div>
              <div
                className="back-to-cdap-link"
                onClick={this.toggleWizard.bind(this, false)}
              >
                Back to Cask Market
              </div>
            </div>
          </div>
        );
      }
    };

    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.Informational.headerlabel') ;
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="informational-wizard"
      >
        {
          this.state.license ?
            getLicenseTemplate()
          :
            getWizardContent()
        }
      </WizardModal>
    );
  }
}
InformationalWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
