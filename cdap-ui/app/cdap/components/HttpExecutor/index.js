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
import MethodSelector from 'components/HttpExecutor/MethodSelector';
import {Provider} from 'react-redux';
import HttpExecutorStore from 'components/HttpExecutor/store/HttpExecutorStore';
import InputPath from 'components/HttpExecutor/InputPath';
import StatusCode from 'components/HttpExecutor/StatusCode';
import HttpResponse from 'components/HttpExecutor/HttpResponse';
import SendButton from 'components/HttpExecutor/SendButton';
import RequestMetadata from 'components/HttpExecutor/RequestMetadata';
import T from 'i18n-react';

const PREFIX = 'features.HttpExecutor';
require('./HttpExecutor.scss');

export default class HttpExecutor extends Component {
  constructor(props) {
    super(props);

  }

  componentDidMount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'none';
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }

  render() {
    return (
      <Provider store={HttpExecutorStore}>
        <div className="http-executor">
          <div className="request-section">
            <MethodSelector />
            <InputPath />

            <SendButton />
          </div>

          <RequestMetadata />

          <div className="response-section">
            <div className="response-header">
              <span className="title">
                {T.translate(`${PREFIX}.responseTitle`)}
              </span>
              <span className="float-xs-right">
                <StatusCode />
              </span>
            </div>

            <HttpResponse />
          </div>
        </div>
      </Provider>
    );
  }
}
