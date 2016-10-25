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
import ArtifactUploadWizardConfig from 'services/WizardConfigs/ArtifactUploadWizardConfig';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import ArtifactUploadActionCreator from 'services/WizardStores/ArtifactUpload/ActionCreator';
import {MyMarketApi} from 'api/market';
import T from 'i18n-react';
import find from 'lodash/find';
import NamespaceStore from 'services/NamespaceStore';

require('./ArtifactUpload.less');

export default class ArtifactUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };

    this.setDefaultConfig();
  }

  setDefaultConfig() {
    const args = this.props.input.action.arguments;
    let config = find(args, {name: 'config'});

    let params = {
      entityName: this.props.input.package.name,
      entityVersion: this.props.input.package.version,
      filename: config.value
    };

    MyMarketApi.getSampleData(params)
      .subscribe((res) => {
        const plugin = res.plugins[0];

        ArtifactUploadStore.dispatch({
          type: ArtifactUploadActions.setName,
          payload: { name: plugin.name }
        });

        ArtifactUploadStore.dispatch({
          type: ArtifactUploadActions.setDescription,
          payload: { description: plugin.description }
        });

        ArtifactUploadStore.dispatch({
          type: ArtifactUploadActions.setClassname,
          payload: { classname: plugin.className }
        });

      });
  }

  componentWillUnmount() {
    ArtifactUploadStore.dispatch({
      type: ArtifactUploadActions.onReset
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

  onSubmit() {
    const state = ArtifactUploadStore.getState();

    let getArtifactNameAndVersion = (nameWithVersion) => {
      if (!nameWithVersion) {
        return {
          version: null,
          name: null
        };
      }

      // core-plugins-3.4.0-SNAPSHOT.jar
      // extracts version from the jar file name. We then get the name of the artifact (that is from the beginning till version beginning)
      let regExpRule = new RegExp('(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.\\-](.*))?$');
      let version = regExpRule.exec(nameWithVersion)[0];
      let name = nameWithVersion.substr(0, nameWithVersion.indexOf(version) -1);
      return { version, name };
    };

    let filename;
    if (state.upload.file.name && state.upload.file.name.length !== 0) {
      filename = state.upload.file.name.split('.jar')[0];
    }
    let {name, version} = getArtifactNameAndVersion(filename);
    let namespace = NamespaceStore.getState().selectedNamespace;

    let url = `/namespaces/${namespace}/artifacts/${name}`;
    let headers = {
      'Artifact-Version': version,
      'Artifact-Extends': state.configure.parentArtifact.join('/'),
      'Artifact-Plugins': [{
        name: state.configure.name,
        type: state.configure.type,
        className: state.configure.classname,
        description: state.configure.description
      }],
      filetype: 'application/octet-stream',
      filename: name
    };

    return ArtifactUploadActionCreator.uploadArtifact({
      url,
      fileContents: state.upload.file,
      headers
    });

  }


  render() {
    let input = this.props.input;
    let pkg = input.package || {};

    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.Informational.headerlabel') ;
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={ArtifactUploadWizardConfig}
          wizardType="ArtifactUpload"
          store={ArtifactUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}
ArtifactUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
