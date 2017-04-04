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

import React, { Component } from 'react';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

require('./DataPrepErrorAlert.scss');

export default class DataPrepErrorAlert extends Component {
  constructor(props) {
    super(props);

    let state = DataPrepStore.getState().error;

    this.state = {
      showError: state.showError,
      message: state.message
    };

    this.dismissError = this.dismissError.bind(this);
  }

  componentWillMount() {
    this.sub = DataPrepStore.subscribe(() => {
      let state = DataPrepStore.getState().error;

      this.setState({
        showError: state.showError,
        message: state.message
      });
    });
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
  }

  dismissError() {
    DataPrepStore.dispatch({
      type: DataPrepActions.dismissError
    });
  }

  render() {
    if (!this.state.showError) { return null; }

    return (
      <div className="dataprep-error-alert-container">
        <div className="error-content">
          {this.state.message}
        </div>
        <div className="close-icon">
          <span
            className="fa fa-times"
            onClick={this.dismissError}
          />
        </div>
      </div>
    );
  }
}
