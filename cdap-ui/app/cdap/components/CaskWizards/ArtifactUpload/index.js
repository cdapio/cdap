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
import PropTypes from 'prop-types';

import React, { Component } from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import ArtifactUploadWizardConfig from 'services/WizardConfigs/ArtifactUploadWizardConfig';
import MarketArtifactUploadWizardConfig from 'services/WizardConfigs/MarketArtifactUploadWizardConfig';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import ArtifactUploadActionCreator from 'services/WizardStores/ArtifactUpload/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

require('./ArtifactUpload.scss');

/**
 * This is the base module that is responsible for uploading plugin and directives
 * both in plus button as well as in market. It falls back to the default success info
 * if none provided as props.
 * One exception: We directly use this component for Driver upload wizard :facepalm:
 * Check ResourceCenter/index.js for wizard mapping.
 */
export default class ArtifactUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      successInfo: {},
    };
    this.eventEmitter = ee(ee);
  }
  componentWillMount() {
    ArtifactUploadStore.dispatch({
      type: ArtifactUploadActions.setType,
      payload: {
        type: 'jdbc',
      },
    });
  }

  componentWillUnmount() {
    ArtifactUploadStore.dispatch({
      type: ArtifactUploadActions.onReset,
    });
  }

  onSubmit() {
    if (!this.props.buildSuccessInfo && this.props.displayCTA) {
      this.buildSuccessInfo();
    }
    return ArtifactUploadActionCreator.uploadArtifact(false).mergeMap((res) => {
      if (this.props.displayCTA === false) {
        this.eventEmitter.emit(globalEvents.CLOSEMARKET);
      }
      this.eventEmitter.emit(globalEvents.ARTIFACTUPLOAD);
      return res; // needs to return something
    });
  }

  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard,
    });
  }

  buildSuccessInfo() {
    let state = ArtifactUploadStore.getState();
    let artifactName = state.configure.name;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.ArtifactUpload.success', { artifactName });
    let subtitle = T.translate('features.Wizard.ArtifactUpload.subtitle');
    let buttonLabel = T.translate('features.Wizard.ArtifactUpload.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    this.setState({
      successInfo: {
        message,
        subtitle,
        buttonLabel,
        buttonUrl: window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
          },
        }),
        handleCallToActionClick: () => {
          /**
           * FIXME (CDAP-15396): Right now we don't know what context we are in (market vs plus button)
           * We should be able to pass on that context from the parent to be able to target specific
           * things in specific environments.
           * Right now this is here to close the modal when user clicks "Create pipeline" on plugin upload
           * while in pipeline studio.
           * */
          this.eventEmitter.emit(globalEvents.CLOSERESOURCECENTER);
        },
        linkLabel,
        linkUrl: `${window.getAbsUIUrl({
          namespaceId: namespace,
        })}/control`,
      },
    });
  }
  render() {
    let input = this.props.input;
    let pkg = input.package || {};
    let headerLabel = input.headerLabel;

    let wizardModalTitle =
      (pkg.label ? pkg.label + ' | ' : '') +
      (headerLabel ? headerLabel : T.translate('features.Wizard.ArtifactUpload.headerlabel'));
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={
            this.props.buildSuccessInfo
              ? MarketArtifactUploadWizardConfig
              : ArtifactUploadWizardConfig
          }
          wizardType="ArtifactUpload"
          store={ArtifactUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          successInfo={this.state.successInfo}
          onClose={this.toggleWizard.bind(this)}
        />
      </WizardModal>
    );
  }
}

ArtifactUploadWizard.defaultProps = {
  input: {
    action: {
      arguments: {},
    },
    package: {},
  },
  displayCTA: true,
};

ArtifactUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  buildSuccessInfo: PropTypes.func,
  hideUploadHelper: PropTypes.bool,
  displayCTA: PropTypes.bool,
};
