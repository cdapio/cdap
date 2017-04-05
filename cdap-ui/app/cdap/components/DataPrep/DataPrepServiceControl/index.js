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

import React, { Component, PropTypes } from 'react';
import enableDataPreparationService from 'components/DataPrep/DataPrepServiceControl/ServiceEnablerUtilities';

export default class DataPrepServiceControl extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false,
      error: null,
      extendedMessage: null
    };

    this.enableService = this.enableService.bind(this);
  }

  componentWillUnmount() {
    if (this.servicePoll && this.servicePoll.dispose) {
      this.servicePoll.dispose();
    }
  }

  enableService() {
    this.setState({loading: true});

    enableDataPreparationService(false)
      .subscribe(() => {
        this.props.onServiceStart();
      }, (err) => {
        this.setState({
          error: err.error,
          extendedMessage: err.extendedMessage,
          loading: false
        });
      });
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <div>
        <h5 className="text-danger text-xs-center">
          {this.state.error}
        </h5>
        <p className="text-dangertext-xs-center">
          {this.state.extendedMessage}
        </p>
      </div>
    );
  }

  render() {
    return (
      <div className="dataprep-container error">
        <h4 className="text-xs-center">
          Data Preparation (Beta) is not enabled. Please enable it.
        </h4>
        <br/>
        <div className="text-xs-center">
          <button
            className="btn btn-primary"
            onClick={this.enableService}
            disabled={this.state.loading}
          >
            {
              !this.state.loading ? 'Enable Data Preparation Service' : (
                <span>
                  <span className="fa fa-spin fa-spinner" /> Enabling...
                </span>
              )
            }
          </button>
        </div>

        <br/>

        {this.renderError()}
      </div>
    );
  }
}
DataPrepServiceControl.propTypes = {
  onServiceStart: PropTypes.func
};
