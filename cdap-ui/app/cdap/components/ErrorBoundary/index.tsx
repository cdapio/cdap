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

import React, { Component, ReactChild } from 'react';
import PropTypes from 'prop-types';
import Page404 from 'components/404';
import Page500 from 'components/500';

const DEFAULT_STATUS_CODE = 500;
export const DEFAULT_ERROR_MESSAGE = 'Something went wrong';

interface IError {
  message: string;
}
interface IErrorInfo {
  entityName: string;
  entityType: string;
  children: ReactChild;
  message: string;
  componentStack: any;
}
export default class ErrorBoundary extends Component {
  public static propTypes = {
    children: PropTypes.node,
  };
  public state = {
    error: false,
    message: '',
    info: {} as IErrorInfo,
    statusCode: DEFAULT_STATUS_CODE,
  };

  public componentDidCatch(error: IError, info: IErrorInfo) {
    let err: { message: string; statusCode: number };
    let message: string;
    let statusCode: number;
    let stackTrace: any;
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
  public render() {
    if (!this.state.error) {
      return this.props.children;
    }
    if (this.state.statusCode === 500) {
      return <Page500 message={this.state.message} stack={this.state.info} refreshFn={null} />;
    }
    if (this.state.statusCode === 404) {
      return (
        <Page404
          message={this.state.message}
          entityType={this.state.info.entityType}
          entityName={this.state.info.entityName}
          children={null}
        />
      );
    }
  }
}
