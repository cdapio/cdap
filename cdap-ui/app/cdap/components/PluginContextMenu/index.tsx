/*
 * Copyright © 2019 Cask Data, Inc.
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

import React from 'react';
import { ContextMenu, IContextMenuOption } from 'components/ContextMenu';
import PropTypes from 'prop-types';
import { CopyToClipBoard } from 'services/Clipboard';

export default function PluginContextMenu({ nodeId, getPluginConfiguration }) {
  const PluginContextMenuOptions: IContextMenuOption[] = [
    {
      name: 'plugin copy',
      label: 'Copy Plugin',
      onClick: () => {
        const text = JSON.stringify(getPluginConfiguration(nodeId));
        CopyToClipBoard(text).then(
          () => console.log('Success now show a tooltip or something to the user'),
          () => console.error('Fail!. Show to the user copy failed')
        );
      },
    },
  ];
  return (
    <React.Fragment>
      <ContextMenu selector={`#${nodeId}`} options={PluginContextMenuOptions} />
    </React.Fragment>
  );
}

(PluginContextMenu as any).propTypes = {
  nodeId: PropTypes.string,
  getPluginConfiguration: PropTypes.func,
};
