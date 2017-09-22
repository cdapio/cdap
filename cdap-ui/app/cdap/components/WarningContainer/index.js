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

import React from 'react';
import T from 'i18n-react';

require('./WarningContainer.scss');

export default function WarningContainer({title, message, children}) {
  return (
    <div className="warning-container">
      <div className="warning-title-container">
        <span className="fa fa-lg fa-exclamation-circle" />
        <span className="warning-title">
          {
            title ?
              title
            :
              T.translate('features.WarningContainer.title')
          }
        </span>
      </div>
      <div className="warning-message-container">
        {message}
      </div>
      {children}
    </div>
  );
}

WarningContainer.defaultProps = {
  title: '',
  message: ''
};

WarningContainer.propTypes = {
  title: PropTypes.string,
  message: PropTypes.string,
  children: PropTypes.node
};
