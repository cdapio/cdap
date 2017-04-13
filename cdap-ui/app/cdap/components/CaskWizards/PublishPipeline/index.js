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
import LicenseStep from 'components/CaskWizards/LicenseStep';
import T from 'i18n-react';

export default class PublishPipelineWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      successInfo: {},
      license: this.props.input.package.license ? true : false
    };

    this.setDefaultConfig();
    this.showWizardContents = this.showWizardContents.bind(this);
    this.eventEmitter = ee(ee);
  }
  componentWillMount() {
    let action = this.props.input.action;
    let filename = head(action.arguments.filter(arg => arg.name === 'config')).value;
    PublishPipelineActionCreator.fetchPipelineConfig({
      entityName: this.props.input.package.name,
      entityVersion: this.props.input.package.version,
      filename
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
  showWizardContents() {
    this.setState({
      license: false
    });
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
  buildSuccessInfo(pipelineName, namespace, draftId) {
    let message = T.translate('features.Wizard.PublishPipeline.success', {pipelineName});
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    let buttonLabel = T.translate('features.Wizard.PublishPipeline.callToAction.customize');
    this.setState({
      successInfo: {
        message: message,
        buttonLabel,
        buttonUrl: window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            draftId
          }
        }),
        linkLabel,
        linkUrl: window.getAbsUIUrl({
          namespaceId: namespace
        })
      }
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
    let draftId;
    if (this.props.input.action.type === 'create_pipeline_draft') {
      return MyUserStoreApi
        .get()
        .flatMap((res) => {
          draftId = shortid.generate();
          draftConfig.__ui__.draftId = draftId;
          res = res || {};
          res.property = res.property || {};
          res.property.hydratorDrafts = res.property.hydratorDrafts || {};
          res.property.hydratorDrafts[currentNamespace] = res.property.hydratorDrafts[currentNamespace] || {};
          res.property.hydratorDrafts[currentNamespace][draftId] = draftConfig;
          return MyUserStoreApi.set({}, res.property);
        })
        .map((res) => {
          this.buildSuccessInfo(name, currentNamespace, draftId);
          this.eventEmitter.emit(globalEvents.PUBLISHPIPELINE);
          return res;
        });
    }
    // this is the case when this is used in an usecase, so
    // PublishPipelineUsecase passes the CTA info to this component
    if (this.props.input.action.type === 'create_pipeline' && this.props.buildSuccessInfo) {
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
          let successInfo = this.props.buildSuccessInfo(name, currentNamespace);
          this.setState({
            successInfo
          });
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
                  <Wizard
                    wizardConfig={PublishPipelineWizardConfig}
                    wizardType="PublishPipeline"
                    onSubmit={this.publishPipeline.bind(this)}
                    successInfo={this.state.successInfo}
                    onClose={this.toggleWizard.bind(this)}
                    store={PublishPipelineWizardStore}
                  />
              }
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
  onClose: PropTypes.func,
  buildSuccessInfo: PropTypes.func,
};
