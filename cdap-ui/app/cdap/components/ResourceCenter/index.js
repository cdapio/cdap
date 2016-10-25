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
import ResourceCenterEntity from '../ResourceCenterEntity';
import StreamCreateWithUploadWizard from 'components/CaskWizards/StreamCreateWithUpload';
import HydratorPipeline from 'components/CaskWizards/HydratorPipeline';
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
import T from 'i18n-react';

require('./ResourceCenter.less');

export default class ResourceCenter extends Component {
  constructor(props) {
    super(props);
    this.state = {
      createStreamWizard: false,
      hydratorPipeline: false,
      entities: [{
        title: T.translate('features.Resource-Center.Stream.label'),
        description: T.translate('features.Resource-Center.Stream.description'),
        actionLabel: T.translate('features.Resource-Center.Stream.actionbtn0'),
        iconClassName: 'fa icon-streams',
        wizardId: 'createStreamWizard'
      }, {
        title: T.translate('features.Resource-Center.Artifact.label'),
        description: T.translate('features.Resource-Center.Artifact.description'),
        actionLabel: T.translate('features.Resource-Center.Artifact.actionbtn0'),
        iconClassName: 'fa icon-artifacts',
        disabled: true
      }, {
        title: T.translate('features.Resource-Center.Application.label'),
        description: T.translate('features.Resource-Center.Application.description'),
        actionLabel: T.translate('features.Resource-Center.Application.actionbtn0'),
        iconClassName: 'fa icon-app',
        disabled: true
      }, {
        title: T.translate('features.Resource-Center.Stream-View.label'),
        description: T.translate('features.Resource-Center.Stream-View.description'),
        actionLabel: T.translate('features.Resource-Center.Stream-View.actionbtn0'),
        iconClassName: 'fa icon-streamview',
        disabled: true
      }, {
        title: T.translate('features.Resource-Center.HydratorPipeline.label'),
        description: T.translate('features.Resource-Center.HydratorPipeline.description'),
        actionLabel: T.translate('features.Resource-Center.HydratorPipeline.actionbtn0'),
        iconClassName: 'fa icon-hydrator',
        wizardId: 'hydratorPipeline',
        disabled: false
      }, {
        title: T.translate('features.Resource-Center.Plugins.label'),
        description: T.translate('features.Resource-Center.Plugins.description'),
        actionLabel: T.translate('features.Resource-Center.Plugins.actionbtn0'),
        iconClassName: 'fa fa-plug',
        disabled: true
      }]
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
      return (<StreamCreateWithUploadWizard
        isOpen={this.state.createStreamWizard}
        store={CreateStreamWithUploadStore}
        onClose={this.toggleWizard.bind(this, 'createStreamWizard')}
        withUploadStep
      />);
    }
    if (this.state.hydratorPipeline) {
      return (
        <HydratorPipeline
          isOpen={this.state.hydratorPipeline}
          onClose={this.toggleWizard.bind(this, 'hydratorPipeline')}
        />
      );
    }
  }
  render(){
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
                />
              ))
          }
        </div>
        { this.getWizardToBeDisplayed() }
      </div>
    );
  }
}
