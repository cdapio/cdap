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
import PublishPipelineWizardStore from 'services/WizardStores/PublishPipeline/PublishPipelineStore';
import PublishPipelineWizardConfig from 'services/WizardConfigs/PublishPipelineWizardConfig';
import PublishPipelineAction from 'services/WizardStores/PublishPipeline/PublishPipelineActions.js';
import PublishPipelineActionCreator from 'services/WizardStores/PublishPipeline/ActionCreator.js';
import head from 'lodash/head';
import shortid from 'shortid';
import MyUserStoreApi from 'api/userstore';
import NamespaceStore from 'services/NamespaceStore';
import {MyPipelineApi} from 'api/pipeline';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

import T from 'i18n-react';

export default class PublishPipelineWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      pipelineNameIsEmpty: false
    };

    this.setDefaultConfig();

    PublishPipelineWizardStore.subscribe(() => {
      let pipelineName = PublishPipelineWizardStore.getState().pipelinemetadata.name;
      if (pipelineName === "") {
        this.setState({pipelineNameIsEmpty: true});
      } else {
        if (this.state.pipelineNameIsEmpty) {
          this.setState({pipelineNameIsEmpty: false});
        }
      }
    });
    this.eventEmitter = ee(ee);
  }

  setDefaultConfig() {
    const args = this.props.input.action.arguments || [];

    args.forEach((arg) => {
      switch (arg.name) {
        case 'name':
          PublishPipelineWizardStore.dispatch({
            type: PublishPipelineAction.setPipelineName,
            payload: {name: arg.value}
          });
          break;
      }
    });
  }
  componentWillMount() {
    let action = this.props.input.action;
    let filename = head(action.arguments.filter(arg => arg.name === 'config')).value;
    PublishPipelineActionCreator
      .fetchPipelineConfig({
        entityName: this.props.input.package.name,
        entityVersion: this.props.input.package.version,
        filename
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
  componentWillReceiveProps({isOpen}) {
    this.setState({
      showWizard: isOpen
    });
  }
  componentWillUnmount() {
    PublishPipelineWizardStore.dispatch({
      type: PublishPipelineAction.onReset
    });
  }
  publishPipeline() {
    let action = this.props.input.action;
    let artifact = head(action.arguments.filter(arg => arg.name === 'artifact')).value;
    let {name, pipelineConfig} = PublishPipelineWizardStore.getState().pipelinemetadata;
    let draftConfig = {
      artifact,
      config: pipelineConfig,
      name: name,
      __ui__: {}
    };
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    if (this.props.input.action.type === 'create_pipeline_draft') {
      return MyUserStoreApi
        .get()
        .flatMap((res) => {
          let draftId = shortid.generate();
          draftConfig.__ui__.draftId = draftId;
          res = res || {};
          res.property = res.property || {};
          res.property.hydratorDrafts = res.property.hydratorDrafts || {};
          res.property.hydratorDrafts[currentNamespace] = res.property.hydratorDrafts[currentNamespace] || {};
          res.property.hydratorDrafts[currentNamespace][draftId] = draftConfig;
          return MyUserStoreApi.set({}, res.property);
        })
        .map((res) => {
          this.eventEmitter.emit(globalEvents.PUBLISHPIPELINE);
          return res;
        });
    }
    if (this.props.input.action.type === 'create_pipeline') {
      return MyPipelineApi
        .publish({
          namespace: currentNamespace,
          appId: name
          }, {
            artifact,
            config: pipelineConfig
          }
        )
        .map((res) => {
          this.eventEmitter.emit(globalEvents.PUBLISHPIPELINE);
          return res;
        });
    }
  }
  render() {
    let input = this.props.input || {};
    let pkg = input.package || {};
    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.PublishPipeline.headerlabel');
    return (
      <div>
        {
          this.state.showWizard ?
            // eww..
            <WizardModal
              title={wizardModalTitle}
              isOpen={this.state.showWizard}
              toggle={this.toggleWizard.bind(this, false)}
              className="create-stream-wizard"
            >
              <Wizard
                wizardConfig={PublishPipelineWizardConfig}
                wizardType="PublishPipeline"
                onSubmit={this.publishPipeline.bind(this)}
                onClose={this.toggleWizard.bind(this)}
                store={PublishPipelineWizardStore}
                finishButtonDisabled={this.state.pipelineNameIsEmpty}
              />
            </WizardModal>
          :
            null
        }
      </div>
    );
  }
}
PublishPipelineWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
