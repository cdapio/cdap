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
require('./ErrorMessageContainer.scss');
import T from 'i18n-react';

export default function ErrorMessageContainer({workspaceName, refreshFn}) {
  const prefix = 'features.DataPrep.DataPrepTable';

  let errorMessageTitle = T.translate(`${prefix}.dataErrorMessageTitle`);
  if (workspaceName) {
    errorMessageTitle = T.translate(`${prefix}.dataErrorMessageTitle2`, {workspaceName});
  }

  return (
    <div className="dataprep-error-container">
      <h4>
        <strong>
          {errorMessageTitle}
        </strong>
      </h4>
      <hr />
      <div className="message-container">
        <div> {T.translate(`${prefix}.suggestionTitle`)} </div>
        <span>
          <span
            className="btn-link"
            onClick={refreshFn}
          >
            {T.translate(`${prefix}.refreshBtnLinkLabel`)}
          </span>

          {T.translate(`${prefix}.suggestion1`)}
        </span>
      </div>
    </div>
  );
}

ErrorMessageContainer.propTypes = {
  workspaceName: PropTypes.string,
  refreshFn: PropTypes.func
};
