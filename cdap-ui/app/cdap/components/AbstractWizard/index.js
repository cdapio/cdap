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

import React from 'react';
import PublishPipelineWizard from 'components/CaskWizards/PublishPipeline';
import PublishPipelineUsecaseWizard from 'components/CaskWizards/PublishPipelineUsecase';
import InformationalWizard from 'components/CaskWizards/Informational';
import ArtifactUploadWizard from 'components/CaskWizards/ArtifactUpload';
import PluginUploadWizard from 'components/CaskWizards/PluginArtifactUpload/PluginUploadWizard';
import DirectiveUploadWizard from 'components/CaskWizards/PluginArtifactUpload/DirectiveUploadWizard';
import ApplicationUploadWizard from 'components/CaskWizards/ApplicationUpload';
import LibraryUploadWizard from 'components/CaskWizards/LibraryUpload';

import MarketArtifactUploadWizard from 'components/CaskWizards/MarketArtifactUpload';
import MarketHydratorPluginUpload from 'components/CaskWizards/MarketHydratorPluginUpload';
import OneStepDeployApp from 'components/CaskWizards/OneStepDeploy/OneStepDeployApp';
import OneStepDeployPlugin from 'components/CaskWizards/OneStepDeploy/OneStepDeployPlugin';
import OneStepDeployPluginUsecase from 'components/CaskWizards/OneStepDeploy/OneStepDeployPluginUsecase';
import OneStepDeployAppUsecase from 'components/CaskWizards/OneStepDeploy/OneStepDeployAppUsecase';
import PublishPipelineStore from 'services/WizardStores/PublishPipeline/PublishPipelineStore';
import AddNamespaceWizard from 'components/CaskWizards/AddNamespace';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import InformationalStore from 'services/WizardStores/Informational/InformationalStore';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ApplicationUploadStore from 'services/WizardStores/ApplicationUpload/ApplicationUploadStore';
import OneStepDeployStore from 'services/WizardStores/OneStepDeploy/OneStepDeployStore';

const WizardTypesMap = {
  create_app: {
    tag: ApplicationUploadWizard,
    store: ApplicationUploadStore,
  },
  create_driver_artifact: {
    tag: MarketArtifactUploadWizard,
    store: ArtifactUploadStore,
  },
  deploy_app: {
    tag: ApplicationUploadWizard,
    store: ApplicationUploadStore,
  },
  create_artifact_rc: {
    tag: ArtifactUploadWizard,
    store: ArtifactUploadStore,
  },
  create_library_rc: {
    tag: LibraryUploadWizard,
    store: ArtifactUploadStore,
  },
  create_plugin_artifact: {
    tag: MarketHydratorPluginUpload,
    store: ArtifactUploadStore,
  },
  create_plugin_artifact_rc: {
    tag: PluginUploadWizard,
    store: ArtifactUploadStore,
  },
  create_app_rc: {
    tag: ApplicationUploadWizard,
    store: ArtifactUploadStore,
  },
  informational: {
    tag: InformationalWizard,
    store: InformationalStore,
  },
  create_pipeline: {
    tag: PublishPipelineUsecaseWizard,
    store: PublishPipelineStore,
  },
  create_pipeline_draft: {
    tag: PublishPipelineWizard,
    store: PublishPipelineStore,
  },
  add_namespace: {
    tag: AddNamespaceWizard,
    store: AddNamespaceStore,
  },
  one_step_deploy_app: {
    tag: OneStepDeployApp,
    store: OneStepDeployStore,
  },
  one_step_deploy_app_usecase: {
    tag: OneStepDeployAppUsecase,
    store: OneStepDeployStore,
  },
  one_step_deploy_plugin: {
    tag: OneStepDeployPlugin,
    store: OneStepDeployStore,
  },
  one_step_deploy_plugin_usecase: {
    tag: OneStepDeployPluginUsecase,
    store: OneStepDeployStore,
  },
  create_directive_artifact_rc: {
    tag: DirectiveUploadWizard,
    store: ArtifactUploadStore,
  },
};

export default function AbstractWizard({
  isOpen,
  onClose,
  wizardType,
  input = null,
  backdrop = true,
  displayCTA = true,
}) {
  if (!isOpen) {
    return null;
  }
  let { tag: Tag, store } = WizardTypesMap[wizardType];
  if (!Tag) {
    return <h1> Wizard Type {wizardType} not found </h1>;
  }
  return React.createElement(Tag, {
    isOpen,
    onClose,
    store,
    input,
    backdrop,
    displayCTA,
  });
}
AbstractWizard.propTypes = {
  isOpen: PropTypes.bool,
  wizardType: PropTypes.string,
  onClose: PropTypes.func,
  input: PropTypes.any,
  backdrop: PropTypes.bool,
  displayCTA: PropTypes.bool,
};
