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

import * as React from 'react';
import {ConnectionType} from 'components/DataPrepConnections/ConnectionType';
import {isNilOrEmpty} from 'services/helpers';
import {Redirect} from 'react-router-dom';
import {getCurrentNamespace} from 'services/NamespaceStore';
import EmptyMessageContainer from 'components/EmptyMessageContainer';
import DataprepBrowserTopPanel from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserTopPanel';
import T from 'i18n-react';

const PREFIX: string = 'features.DataPrepConnections.NoDefaultConnection';

interface IDefaultConnection {
  name: string;
  type: ConnectionType;
}
interface INoDefaultConnectionProps {
  defaultConnection: IDefaultConnection;
  showAddConnectionPopover: () => void;
  toggleSidepanel: (e: React.MouseEvent<HTMLElement>) => void;
}
const NoDefaultConnection: React.SFC<INoDefaultConnectionProps> = ({
  defaultConnection,
  showAddConnectionPopover,
  toggleSidepanel,
}) => {
  if (
    isNilOrEmpty(defaultConnection) ||
    (defaultConnection && isNilOrEmpty(defaultConnection.name))
  ) {
    return (
      <div>
        <DataprepBrowserTopPanel
          allowSidePanelToggle={true}
          toggle={toggleSidepanel}
          browserTitle={T.translate(`${PREFIX}.title`).toString()}
        />

        <EmptyMessageContainer title={T.translate(`${PREFIX}.title`)}>
          <span>
            <br />
            <ul>
              <li>
                <span
                  className="link-text"
                  onClick={showAddConnectionPopover}
                >Create</span>
                <span>a new connection; or</span>
              </li>
              <li>
                <span>Click on an existing connection to browse</span>
              </li>
            </ul>
          </span>
        </EmptyMessageContainer>
      </div>
    );
  }
  const { name: connectionId, type } = defaultConnection;
  const connectionType = type.toLowerCase();
  const namespace = getCurrentNamespace();
  const BASEPATH = `/ns/${namespace}/connections`;
  return (
    <Redirect to={`${BASEPATH}/${connectionType}/${connectionId}`} />
  );
};
export default NoDefaultConnection;
