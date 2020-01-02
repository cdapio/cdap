/*
 * Copyright © 2018 Cask Data, Inc.
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
import classnames from 'classnames';
require('./Page500ErrorStack.scss');

export default class Page500ErrorStack extends Component {
  static propTypes = {
    stack: PropTypes.string,
    message: PropTypes.string,
  };

  state = {
    showError: false,
  };

  toggleShowError = () => {
    this.setState({
      showError: !this.state.showError,
    });
  };

  render() {
    let { stack } = this.props;
    return (
      <div className="page-500-error-stack">
        <div className="btn btn-link" onClick={this.toggleShowError}>
          {this.state.showError ? 'Hide error stack' : 'Show error stack'}
        </div>
        <div
          className={classnames('stack-section', {
            open: this.state.showError,
          })}
        >
          <strong>{this.props.message}</strong>
          <pre className="text-danger">
            {typeof stack === 'object' ? JSON.stringify(stack, null, 2) : stack}
          </pre>
        </div>
      </div>
    );
  }
}
