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
import StreamCreateWithUploadWizard from 'components/CaskWizards/StreamCreateWithUpload';
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
import NamespaceStore from 'services/NamespaceStore';
import AbstractWizard from 'components/AbstractWizard';
import T from 'i18n-react';

require('./ResourceCenter.scss');
/*
  TODO: Stream views:
  {
    title: T.translate('features.Resource-Center.Stream-View.label'),
    description: T.translate('features.Resource-Center.Stream-View.description'),
    actionLabel: T.translate('features.Resource-Center.Stream-View.actionbtn0'),
    iconClassName: 'fa icon-streamview',
    disabled: true
  }
*/
export default class ResourceCenter extends Component {
  constructor(props) {
    super(props);
    this.state = {
      createStreamWizard: false,
      entities: [
        {
          // Pipeline
          title: T.translate('features.Resource-Center.HydratorPipeline.label'),
          description: T.translate('features.Resource-Center.HydratorPipeline.description'),
          actionLabel: T.translate('features.Resource-Center.HydratorPipeline.actionbtn0'),
          iconClassName: 'icon-pipelines',
          disabled: false,
          actionLink: window.getHydratorUrl({
            stateName: 'hydrator.create',
            stateParams: {
              namespace: NamespaceStore.getState().selectedNamespace,
              artifactType: 'cdap-data-pipeline'
            }
          })
        },
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
        }
      ]
    };
  }
  toggleWizard(wizardName) {
    this.setState({
      [wizardName]: !this.state[wizardName]
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
  }
  render() {
    return (
      <div>
        <div className="cask-resource-center">
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
                  disabled={entity.disabled}
                  onClick={this.toggleWizard.bind(this, entity.wizardId)}
                  actionLink={entity.actionLink}
                />
              ))
          }
        </div>
        { this.getWizardToBeDisplayed() }
      </div>
    );
  }
}
