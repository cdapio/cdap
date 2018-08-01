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
import React from 'react';
import {isNilOrEmpty} from 'services/helpers';
import {DEFAULT_ERROR_MESSAGE} from 'components/ErrorBoundary';
import Page500ErrorStack from 'components/500/Page500ErrorStack';
require('./500.scss');

export default function Page500({message, stack}) {
  return (
    <div className="page-500">
      <img src="/cdap_assets/img/404.png" />
      <h4>
        <strong>Sorry, something unexpected happend.</strong>
        <small>Bug/JIRA integration coming soon...</small>
        <small>
          Please
          <a href={window.location.href}> click here </a>
          to reload the application
        </small>
      </h4>
      {
        isNilOrEmpty(stack) ?
          null
        :
          <Page500ErrorStack
            stack={stack}
            message={isNilOrEmpty(message) ? DEFAULT_ERROR_MESSAGE : message}
          />
      }
    </div>
  );
}
Page500.propTypes = {
  message: PropTypes.string,
  stack: PropTypes.object
};
