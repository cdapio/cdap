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

import React, {Component} from 'react';
import Alert from 'components/Alert';
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import {resetError} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';

export default class RulesEngineAlert extends Component {

  getDefaultState = () => {
    return {
      showAlert: false,
      alertMessage: null,
      alertType: null
    };
  };

  state = this.getDefaultState();

  componentDidMount() {
    RulesEngineStore.subscribe(() => {
      let {error} = RulesEngineStore.getState();
      if (error.showError) {
        this.setState({
          alertMessage: error.message,
          showAlert: true,
          alertType: 'error'
        });
      } else {
        this.setState(this.getDefaultState());
      }
    });
  }

  render() {
    if (!this.state.showAlert) {
      return null;
    }
    return (
      <Alert
        message={this.state.alertMessage}
        type={this.state.alertType}
        showAlert={this.state.showAlert}
        onClose={resetError}
      />
    );
  }
}
