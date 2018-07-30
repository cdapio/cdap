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

export default function Page500({message, stack}) {
  return (
    <div className="page-not-found">
      <img src="/cdap_assets/img/404.png" />
      <h1>
        <strong>
          {
            isNilOrEmpty(message) ?
              DEFAULT_ERROR_MESSAGE
            :
              message
          }
        </strong>
      </h1>
      {
        isNilOrEmpty(stack) ?
          null
        :
          <div className="message-section">
            <pre>{JSON.stringify(stack, null, 2)}</pre>
          </div>
      }
    </div>
  );
}
Page500.propTypes = {
  message: PropTypes.string,
  stack: PropTypes.object
};
