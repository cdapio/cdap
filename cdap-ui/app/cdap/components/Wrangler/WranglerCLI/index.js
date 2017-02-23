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
import WranglerStore from 'components/Wrangler/store';
import WranglerActions from 'components/Wrangler/store/WranglerActions';
import MyWranglerApi from 'api/wrangler';
import WranglerAutoComplete from 'components/Wrangler/AutoComplete';
require('./WranglerCLI.scss');

export default class WranglerCLI extends Component {
  constructor(props) {
    super(props);

    this.state = {
      directiveInput: '',
      error: null,
      autoCompleteOpen: false
    };

    this.handleDirectiveChange = this.handleDirectiveChange.bind(this);
    this.toggleAutoComplete = this.toggleAutoComplete.bind(this);
    this.dismissError = this.dismissError.bind(this);
    this.handlePaste = this.handlePaste.bind(this);
    this.execute = this.execute.bind(this);
  }

  componentDidMount() {
    if (this.directiveRef) {
      this.directiveRef.focus();
    }
  }

  handleDirectiveChange(e) {
    this.setState({
      directiveInput: e.target.value,
      autoCompleteOpen: true
    });
  }

  toggleAutoComplete() {
    this.setState({autoCompleteOpen: !this.state.autoCompleteOpen});
  }

  execute(addDirective) {
    if (addDirective.length === 0) { return; }

    let store = WranglerStore.getState().wrangler;
    let updatedDirectives = store.directives.concat(addDirective);

    let workspaceId = store.workspaceId;

    let params = {
      namespace: 'default',
      workspaceId: workspaceId,
      limit: 100,
      directive: updatedDirectives
    };

    MyWranglerApi.execute(params)
      .subscribe((res) => {
        this.setState({
          error: null,
          directiveInput: ''
        });

        WranglerStore.dispatch({
          type: WranglerActions.setDirectives,
          payload: {
            data: res.value,
            headers: res.header,
            directives: updatedDirectives
          }
        });
      }, (err) => {
        this.setState({
          error: err.message || err.response.message
        });
      });

  }

  dismissError() {
    this.setState({error: null});
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <div className="error-bar">
        <span className="content">
          {this.state.error}
        </span>

        <span
          className="fa fa-times float-xs-right"
          onClick={this.dismissError}
        />
      </div>
    );
  }

  handlePaste(e) {
    let data = e.clipboardData.getData('Text');
    data = data.split('\n')
      .filter((row) => {
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
      <div className="wrangler-cli">

        {this.renderError()}

        <WranglerAutoComplete
          input={this.state.directiveInput}
          onRowClick={this.handleDirectiveChange}
          inputRef={this.directiveRef}
          toggle={this.toggleAutoComplete}
          isOpen={this.state.autoCompleteOpen}
          hasError={this.state.error}
          execute={this.execute}
        />

        <div className="input-container">
          <strong>$</strong>
          <div className="directive-input">
            <input
              type="text"
              className="form-control mousetrap"
              id="directive-input"
              value={this.state.directiveInput}
              onChange={this.handleDirectiveChange}
              ref={(ref) => this.directiveRef = ref}
              onPaste={this.handlePaste}
            />
          </div>
        </div>
      </div>
    );
  }
}
