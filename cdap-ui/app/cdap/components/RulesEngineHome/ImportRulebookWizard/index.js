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

import React, {Component, PropTypes} from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import UploadFile from 'services/upload-file';
import NamespaceStore from 'services/NamespaceStore';
import {setActiveRulebook, getRuleBooks} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import Rx from 'rx';
import ImportRulebookWizardConfig from 'components/RulesEngineHome/ImportRulebookWizard/ImportRulebookWizardConfig';
import ImportRulebookStore from 'components/RulesEngineHome/ImportRulebookWizard/ImportRulebookStore';
import T from 'i18n-react';

require('./ImportRulebookWizard.scss');
const PREFIX = 'features.RulesEngine.ImportRulebook';

export default class ImportRulebookWizard extends Component {

  static propTypes = {
    isOpen: PropTypes.book,
    onClose: PropTypes.func
  };

  componentWillReceiveProps(nextProps) {
    if (nextProps.isOpen !== this.state.isOpen) {
      this.setState({
        isOpen: nextProps.isOpen
      });
    }
  }

  state = {
    isOpen: this.props.isOpen || false,
    file: ''
  };

  setFile = ([file]) => {
    this.setState({
      file
    });
  }

  toggleWizard = () => {
    this.setState({
      isOpen: !this.state.showWizard
    });
    if (this.props.onClose) {
      this.props.onClose();
    }
  }

  onSubmit = () => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let url = `/namespaces/${namespace}/apps/yare/services/service/methods/rulebooks`;
    let headers = {
      'content-type': 'application/rules-engine'
    };
    let data = ImportRulebookStore.getState().upload.file.contents;
    return UploadFile({
      url,
      fileContents: data,
      headers
    })
      .flatMap(
        (res) => {
          let response;
          let activeRulebook;
          try {
            response = JSON.parse(res);
            activeRulebook= response.values[0];
            setActiveRulebook(activeRulebook);
          } catch (e) {
            console.log('Unable to parse response. Just getting rulebooks', e);
          }
          getRuleBooks();
          this.toggleWizard();
          return Rx.Observable.create((obs) => {
            obs.onNext();
          });
        }
      );
  }

  render() {
    return (
      <WizardModal
        title={T.translate(`${PREFIX}.shorttitle`)}
        isOpen={this.state.isOpen}
        toggle={this.toggleWizard.bind(this, false)}
        className="rulebook-upload-wizard"
      >
        <Wizard
          wizardConfig={ImportRulebookWizardConfig}
          store={ImportRulebookStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)
        }/>
      </WizardModal>
    );
  }
}
