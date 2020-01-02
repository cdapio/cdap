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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import Page404 from 'components/404';
import Page500 from 'components/500';

const DEFAULT_STATUS_CODE = 500;
export const DEFAULT_ERROR_MESSAGE = 'Something went wrong';
export default class ErrorBoundary extends Component {
  static propTypes = {
    children: PropTypes.node,
  };
  state = {
    error: false,
    message: '',
    info: {},
  };
  componentDidCatch(error, info) {
    let err, message, statusCode, stackTrace;
    if (error instanceof Error) {
      message = error.message;
      statusCode = DEFAULT_STATUS_CODE;
    } else {
      try {
        err = JSON.parse(error.message);
        message = err.message || DEFAULT_ERROR_MESSAGE;
        statusCode = err.statusCode || DEFAULT_STATUS_CODE;
      } catch (e) {
        message = DEFAULT_ERROR_MESSAGE;
        statusCode = DEFAULT_STATUS_CODE;
      }
    }
    stackTrace = info.componentStack;
    this.setState({
      error: true,
      message,
      statusCode,
      info: stackTrace,
    });
  }
  render() {
    if (!this.state.error) {
      return this.props.children;
    }
    if (this.state.statusCode === 500) {
      return <Page500 message={this.state.message} stack={this.state.info} />;
    }
    if (this.state.statusCode === 404) {
      return (
        <Page404 entityType={this.state.info.entityType} entityName={this.state.info.entityName} />
      );
    }
  }
}
