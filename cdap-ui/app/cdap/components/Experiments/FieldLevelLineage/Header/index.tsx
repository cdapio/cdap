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
import { Consumer } from '../FllContext';

interface IHeaderProps {
  type: string;
  first: number;
  total: number;
}

function Header({ type, first, total }: IHeaderProps) {
  return (
    <Consumer>
      {({ numTables, target }) => {
        let last;
        if (type === ('impact' || 'cause')) {
          last = first + numTables - 1 <= total ? first + numTables - 1 : total;
        } else {
          last = total;
        }

        const header =
          type === 'target'
            ? 'Select a field to view lineage and operations'
            : `Datasets used as ${type} by ${target}`;
        const subheaderRange = `${first} to ${last}`;
        const units = type === 'target' ? 'fields' : 'datasets';

        return (
          <div className={`${type} header`}>
            <div className="main-header">{header}</div>
            <div>{`Viewing ${subheaderRange} of ${total} ${units}`}</div>
          </div>
        );
      }}
    </Consumer>
  );
}

export default Header;
