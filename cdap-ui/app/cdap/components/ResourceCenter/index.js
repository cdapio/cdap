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

import React, {Component} from 'react';
import ReactDOM from 'react-dom';
import ResourceCenterEntity from 'components/ResourceCenterEntity';
import ResourceCenterPipelineEntity from 'components/ResourceCenterEntity/ResourceCenterPipelineEntity';
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
import AbstractWizard from 'components/AbstractWizard';
import CardActionFeedback from 'components/CardActionFeedback';
import T from 'i18n-react';
import StreamCreateWithUploadWizard from 'components/CaskWizards/StreamCreateWithUpload';

require('./ResourceCenter.scss');

export default class ResourceCenter extends Component {
  constructor(props) {
    super(props);
    this.state = {
      createStreamWizard: false,
      error: null,
      entities: [
        {
          // Application
          title: T.translate('features.Resource-Center.Application.label'),
          description: T.translate('features.Resource-Center.Application.description'),
          actionLabel: T.translate('features.Resource-Center.Application.actionbtn0'),
          iconClassName: 'icon-app',
          wizardId: 'createApplicationWizard'
        },
        {
          // Plugin
          title: T.translate('features.Resource-Center.Plugins.label'),
          description: T.translate('features.Resource-Center.Plugins.description'),
          actionLabel: T.translate('features.Resource-Center.Plugins.actionbtn0'),
          iconClassName: 'icon-plug',
          wizardId: 'createPluginArtifactWizard'
        },
        {
          // Driver
          title: T.translate('features.Resource-Center.Artifact.label'),
          description: T.translate('features.Resource-Center.Artifact.description'),
          actionLabel: T.translate('features.Resource-Center.Artifact.actionbtn0'),
          iconClassName: 'icon-artifacts',
          wizardId: 'createArtifactWizard'
        },
        {
          // Library
          title: T.translate('features.Resource-Center.Library.label'),
          description: T.translate('features.Resource-Center.Library.description'),
          actionLabel: T.translate('features.Resource-Center.Library.actionbtn0'),
          iconClassName: 'icon-library',
          wizardId: 'createLibraryWizard'
        },
        {
          // Stream
          title: T.translate('features.Resource-Center.Stream.label'),
          description: T.translate('features.Resource-Center.Stream.description'),
          actionLabel: T.translate('features.Resource-Center.Stream.actionbtn0'),
          iconClassName: 'icon-streams',
          wizardId: 'createStreamWizard'
        },
        {
          // Microservice
          title: T.translate('features.Resource-Center.Microservice.label'),
          description: T.translate('features.Resource-Center.Microservice.description'),
          actionLabel: T.translate('features.Resource-Center.Microservice.actionbtn0'),
          iconClassName: 'icon-app',
          wizardId: 'createMicroserviceWizard'
        }
      ]
    };
  }
  onError = (error) => {
    this.setState({
      error
    });
  };
  toggleWizard(wizardName) {
    this.setState({
      [wizardName]: !this.state[wizardName],
      error: null
    });
  }
  closeWizard(wizardContainer) {
    ReactDOM.unmountComponentAtNode(wizardContainer);
  }
  getWizardToBeDisplayed() {
    if (this.state.createStreamWizard) {
      return (
        <StreamCreateWithUploadWizard
          isOpen={this.state.createStreamWizard}
          store={CreateStreamWithUploadStore}
          onClose={this.toggleWizard.bind(this, 'createStreamWizard')}
          withUploadStep
        />
      );
    }
    if (this.state.createApplicationWizard) {
      return (
        <AbstractWizard
          wizardType="create_app_rc"
          isOpen={true}
          input={{headerLabel: T.translate('features.Resource-Center.Application.modalheadertitle')}}
          onClose={this.toggleWizard.bind(this, 'createApplicationWizard')}
        />
      );
    }
    if (this.state.createArtifactWizard) {
      return (
        <AbstractWizard
          isOpen={true}
          wizardType="create_artifact_rc"
          input={{headerLabel: T.translate('features.Resource-Center.Artifact.modalheadertitle')}}
          onClose={this.toggleWizard.bind(this, 'createArtifactWizard')}
        />
      );
    }
    if (this.state.createPluginArtifactWizard) {
      return (
        <AbstractWizard
          isOpen={true}
          wizardType="create_plugin_artifact_rc"
          input={{headerLabel: T.translate('features.Resource-Center.Plugins.modalheadertitle')}}
          onClose={this.toggleWizard.bind(this, 'createPluginArtifactWizard')}
        />
      );
    }
    if (this.state.createLibraryWizard) {
      return (
        <AbstractWizard
          isOpen={true}
          wizardType="create_library_rc"
          input={{headerLabel: T.translate('features.Resource-Center.Library.modalheadertitle')}}
          onClose={this.toggleWizard.bind(this, 'createLibraryWizard')}
        />
      );
    }
    if (this.state.createMicroserviceWizard) {
      return (
        <AbstractWizard
          isOpen={true}
          wizardType="create_microservice_rc"
          input={{headerLabel: T.translate('features.Resource-Center.Microservice.modalheadertitle')}}
          onClose={this.toggleWizard.bind(this, 'createMicroserviceWizard')}
        />
      );
    }
  }
  renderError() {
    if (!this.state.error) { return null; }

    return (
      <CardActionFeedback
        type="DANGER"
        message={this.state.error}
      />
    );
  }
  render() {
    return (
      <div>
        <div className="cask-resource-center">
          <ResourceCenterPipelineEntity
            onError={this.onError}
          />
          {
            this.state
              .entities
              .map((entity, index) => (
                <ResourceCenterEntity
                  title={entity.title}
                  description={entity.description}
                  actionLabel={entity.actionLabel}
                  iconClassName={entity.iconClassName}
                  key={index}
                  onClick={this.toggleWizard.bind(this, entity.wizardId)}
                />
              ))
          }
        </div>
        {this.renderError()}
        { this.getWizardToBeDisplayed() }
      </div>
    );
  }
}
