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

import * as React from 'react';
import T from 'i18n-react';
import { deleteDraft } from 'components/PipelineList/DraftPipelineView/store/ActionCreator';
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';
import ActionsPopover, { IAction } from 'components/ActionsPopover';

interface IProps {
  draft: IDraft;
}

const DraftActions: React.SFC<IProps> = ({ draft }) => {
  const actions: IAction[] = [
    {
      label: T.translate('commons.export'),
      disabled: true,
    },
    {
      label: 'separator',
    },
    {
      label: T.translate('commons.delete'),
      actionFn: deleteDraft.bind(null, draft),
      className: 'delete',
    },
  ];

  return (
    <div className="table-column action text-xs-center">
      <ActionsPopover actions={actions} />
    </div>
  );
};

export default DraftActions;
