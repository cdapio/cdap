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

import PropTypes from 'prop-types';
import React, { Component } from 'react';
import DataPrepAutoComplete from 'components/DataPrep/AutoComplete';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
require('./DataPrepCLI.scss');

export default class DataPrepCLI extends Component {
  static propTypes = {
    disabled: PropTypes.bool,
  };

  constructor(props) {
    super(props);

    this.state = {
      directiveInput: '',
      error: null,
      autoCompleteOpen: false,
      currentWorkspace: null,
    };

    this.handleDirectiveChange = this.handleDirectiveChange.bind(this);
    this.toggleAutoComplete = this.toggleAutoComplete.bind(this);
    this.dismissError = this.dismissError.bind(this);
    this.handlePaste = this.handlePaste.bind(this);
    this.execute = this.execute.bind(this);

    this.sub = DataPrepStore.subscribe(() => {
      let { error, dataprep } = DataPrepStore.getState();
      let newState = {
        error: error.cliError,
      };
      if (dataprep.workspaceId !== this.state.currentWorkspace) {
        newState = {
          ...newState,
          currentWorkspace: dataprep.workspaceId,
          directiveInput: '',
        };
      }
      this.setState(newState);
    });
  }

  componentDidMount() {
    if (this.directiveRef) {
      this.directiveRef.focus();
    }
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
  }

  handleDirectiveChange(e) {
    this.setState({
      directiveInput: e.target.value,
      autoCompleteOpen: true,
    });
  }

  toggleAutoComplete() {
    this.setState({ autoCompleteOpen: !this.state.autoCompleteOpen });
  }

  execute(addDirective) {
    execute(addDirective).subscribe(
      () => {
        this.setState({
          directiveInput: '',
        });
      },
      (err) => {
        DataPrepStore.dispatch({
          type: DataPrepActions.setCLIError,
          payload: {
            message: err.message || err.response.message,
          },
        });
      }
    );
  }

  dismissError() {
    DataPrepStore.dispatch({
      type: DataPrepActions.setCLIError,
      payload: {
        message: null,
      },
    });
  }

  renderError() {
    if (!this.state.error) {
      return null;
    }

    return (
      <div className="error-bar">
        <span className="content">{this.state.error}</span>

        <span className="fa fa-times float-right" onClick={this.dismissError} />
      </div>
    );
  }

  handlePaste(e) {
    let data = e.clipboardData.getData('Text');
    data = data.split('\n').filter((row) => {
      // filter out empty rows
      return row.trim().length > 0;
    });

    if (data.length > 1) {
      e.preventDefault();
      this.execute(data);
    }
  }

  render() {
    return (
      <div className="dataprep-cli">
        {this.renderError()}

        <DataPrepAutoComplete
          input={this.state.directiveInput}
          onRowClick={this.handleDirectiveChange}
          inputRef={this.directiveRef}
          toggle={this.toggleAutoComplete}
          isOpen={this.state.autoCompleteOpen}
          hasError={this.state.error}
          execute={this.execute}
        />

        <div className="input-container clearfix">
          <strong className="float-left">$</strong>
          <div className="directive-input">
            <input
              type="text"
              className="form-control mousetrap"
              id="directive-input"
              value={this.state.directiveInput}
              onChange={this.handleDirectiveChange}
              ref={(ref) => (this.directiveRef = ref)}
              onPaste={this.handlePaste}
              disabled={this.props.disabled}
            />
          </div>
        </div>
      </div>
    );
  }
}
