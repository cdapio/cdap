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

import React, {PropTypes} from 'react';
import {setWorkspace} from 'components/DataPrep/store/DataPrepActionCreator';
require('./ErrorMessageContainer.scss');
import T from 'i18n-react';

export default function ErrorMessageContainer({workspaceId, workspaceName}) {
  return (
    <div className="dataprep-error-container">
      <h4>
        <strong>
          {T.translate(`features.DataPrep.DataPrepTable.dataErrorMessageTitle`, {workspaceName})}
        </strong>
      </h4>
      <hr />
      <div className="message-container">
        <div> {T.translate(`features.DataPrep.DataPrepTable.suggestionTitle`)} </div>
        <span>
          <span
            className="btn-link"
            onClick={() => setWorkspace(workspaceId).subscribe()}
          >
            {T.translate(`features.DataPrep.DataPrepTable.refreshBtnLinkLabel`)}
          </span>

          {T.translate(`features.DataPrep.DataPrepTable.suggestion1`)}
        </span>
      </div>
    </div>
  );
}

ErrorMessageContainer.propTypes = {
  workspaceId: PropTypes.string,
  workspaceName: PropTypes.string
};
