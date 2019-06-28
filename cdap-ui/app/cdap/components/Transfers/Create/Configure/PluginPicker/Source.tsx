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
import AbstractPicker from './AbstractPicker';
import { transfersCreateConnect } from 'components/Transfers/Create/context';

const Sources = [
  {
    label: 'MySQL',
  },
  {
    label: 'Oracle',
    disabled: true,
  },
  {
    label: 'Microsoft SQL Server',
    disabled: true,
  },
  {
    label: 'SAP HANA',
    disabled: true,
  },
  {
    label: 'Teradata',
    disabled: true,
  },
  {
    label: 'Postgres',
    disabled: true,
  },
  {
    label: 'Redis',
    disabled: true,
  },
  {
    label: 'Amazon Aurora',
    disabled: true,
  },
  {
    label: 'IBM DB2',
    disabled: true,
  },
  {
    label: 'Amazon Redshift',
    disabled: true,
  },
  {
    label: 'Snowflake',
    disabled: true,
  },
  {
    label: 'Netezza',
    disabled: true,
  },
  {
    label: 'MongoDB',
    disabled: true,
  },
];

interface IProps {
  next: () => void;
}

const SourceView: React.SFC<IProps> = ({ next }) => {
  function onClickHandler(plugin) {
    if (plugin.disabled) {
      return;
    }

    next();
  }

  return <AbstractPicker plugins={Sources} onClick={onClickHandler} />;
};

const Source = transfersCreateConnect(SourceView);
export default Source;
