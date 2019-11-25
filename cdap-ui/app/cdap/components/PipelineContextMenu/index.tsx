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
import WranglerConnection from 'components/PipelineContextMenu/WranglerConnection';
import If from 'components/If';
export default function PipelineContextMenu() {
  const [showWranglerModal, setShowWranglerModal] = React.useState(false);

  const menuOptions: IContextMenuOption[] = [
    {
      name: 'add-wrangler-source',
      label: 'Add a wrangler source',
      onClick: () => setShowWranglerModal(!showWranglerModal),
    },
    {
      name: 'pipeline node copy',
      label: 'Copy',
      onClick: () => console.log('Copying node(s)'),
      disabled: true,
    },
    {
      name: 'pipeline node paste',
      label: 'Paste',
      onClick: () => console.log('Pasting node(s)'),
      disabled: true,
    }
  ];
  return (
    <React.Fragment>
      <ContextMenu
        selector="#diagram-container"
        options={menuOptions}
      />
      <If condition={showWranglerModal}>
        <WranglerConnection onModalClose={() => setShowWranglerModal(!showWranglerModal)} />
      </If>
    </React.Fragment>
  );
} 
