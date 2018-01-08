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

import React, { Component } from 'react';
import DirectiveUploadWizard from 'components/CaskWizards/PluginArtifactUpload/DirectiveUploadWizard';
import T from 'i18n-react';
import PlusButton from 'components/PlusButton';

export default class DataPrepPlusButton extends Component {
  constructor(props) {
    super(props);

    this.state = {
      showCustomDirective: false,
      showSuccessAlert: false
    };

    this.onDirectiveClick = this.onDirectiveClick.bind(this);
    this.onDirectiveSubmit = this.onDirectiveSubmit.bind(this);
  }

  onDirectiveClick = () => {
    this.setState({showCustomDirective: !this.state.showCustomDirective});
  }

  PLUSBUTTONCONTEXTMENUITEMS = [
    {
      label: 'Add directive',
      onClick: this.onDirectiveClick
    }
  ];

  onDirectiveSubmit() {
    this.setState({
      showCustomDirective: false,
      showSuccessAlert: true
    }, () => {
      setTimeout(() => {
        this.setState({
          showSuccessAlert: false
        });
      }, 3000);
    });
  }

  renderSuccessAlert() {
    if (!this.state.showSuccessAlert) { return null; }

    return (
      <div className="success-alert">
        {T.translate('features.DataPrep.TopPanel.PlusButton.successMessage')}
      </div>
    );
  }

  renderDirectiveWizard() {
    if (!this.state.showCustomDirective) { return null; }

    let input = {
      headerLabel: T.translate('features.Resource-Center.Directive.modalheadertitle')
    };

    return (
      <DirectiveUploadWizard
        isOpen={this.state.showCustomDirective}
        onClose={() => this.setState({showCustomDirective: false})}
        onSubmit={this.onDirectiveSubmit}
        input={input}
        displayCTA={false}
      />
    );
  }

  render() {
    return (
      <div>
        {this.renderSuccessAlert()}
        <PlusButton
          mode={PlusButton.MODE.resourcecenter}
          contextItems={this.PLUSBUTTONCONTEXTMENUITEMS}
        />
        {this.renderDirectiveWizard()}
      </div>
    );
  }
}
