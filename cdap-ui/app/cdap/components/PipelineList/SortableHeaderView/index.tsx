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

import * as React from 'react';
import { SORT_ORDER } from 'components/PipelineList/DeployedPipelineView/store';
import If from 'components/If';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import classnames from 'classnames';

interface ISortableHeaderProps {
  sortColumn: string;
  sortOrder: SORT_ORDER;
  columnName: string;
  disabled: boolean;
  setSort: (columnName: string) => void;
}

const PREFIX = 'features.PipelineList';

const SortableHeaderView: React.SFC<ISortableHeaderProps> = ({
  sortColumn,
  sortOrder,
  columnName,
  disabled,
  setSort,
}) => {
  function handleClick() {
    if (disabled) {
      return;
    }

    setSort(columnName);
  }

  return (
    <strong className={classnames({ sortable: !disabled })} onClick={handleClick}>
      {T.translate(`${PREFIX}.${columnName}`)}

      <If condition={sortColumn === columnName}>
        <span className="fa fa-lg">
          <IconSVG name={sortOrder === SORT_ORDER.asc ? 'icon-caret-down' : 'icon-caret-up'} />
        </span>
      </If>
    </strong>
  );
};

export default SortableHeaderView;
