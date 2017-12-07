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
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import PlusButtonStore from 'services/PlusButtonStore';
import PlusButtonModal from 'components/PlusButtonModal';
import DirectiveUploadWizard from 'components/CaskWizards/PluginArtifactUpload/DirectiveUploadWizard';
import T from 'i18n-react';

export default class DataPrepPlusButton extends Component {
  constructor(props) {
    super(props);

    this.state = {
      showResourceCenter: false,
      showCustomDirective: false,
      showSuccessAlert: false
    };

    this.onDirectiveClick = this.onDirectiveClick.bind(this);
    this.onMoreClick = this.onMoreClick.bind(this);
    this.onDirectiveSubmit = this.onDirectiveSubmit.bind(this);
  }

  componentDidMount() {
    this.plusButtonSubscription = PlusButtonStore.subscribe(() => {
      let modalState = PlusButtonStore.getState().modalState;
      this.setState({
        showResourceCenter: modalState
      });
    });
  }
  componentWillUnmount() {
    if (this.plusButtonSubscription) {
      this.plusButtonSubscription();
    }
  }

  onDirectiveClick() {
    this.setState({showCustomDirective: !this.state.showCustomDirective});
  }

  onMoreClick() {
    this.setState({showResourceCenter: !this.state.showResourceCenter});
  }

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
        isOpen={true}
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
        <div className="dataprep-plus-button">
          <UncontrolledDropdown>
            <DropdownToggle>
              <img
                id="resource-center-btn"
                className="button-container"
                src="/cdap_assets/img/plus_ico.svg"
              />
            </DropdownToggle>
            <DropdownMenu
              className="plus-button-dropdown"
              right
            >
              <DropdownItem
                onClick={this.onDirectiveClick}
              >
                {T.translate('features.DataPrep.TopPanel.PlusButton.addDirective')}
              </DropdownItem>

              <hr />

              <DropdownItem
                onClick={this.onMoreClick}
              >
                {T.translate('features.DataPrep.TopPanel.PlusButton.addOtherEntities')}
              </DropdownItem>
            </DropdownMenu>
          </UncontrolledDropdown>

          <PlusButtonModal
            isOpen={this.state.showResourceCenter}
            onCloseHandler={this.onMoreClick.bind(this)}
            mode="resourcecenter"
          />

          {this.renderDirectiveWizard()}
        </div>
      </div>
    );
  }
}
