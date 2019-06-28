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

const Targets = [
  {
    label: 'Google BigQuery',
  },
  {
    label: 'Google Cloud Spanner',
    disabled: true,
  },
  {
    label: 'Google Cloud Storage',
    disabled: true,
  },
  {
    label: 'Cloud SQL MySQL',
    disabled: true,
  },
  {
    label: 'Cloud SQL Postgres',
    disabled: true,
  },
  {
    label: 'Cloud Bigtable',
    disabled: true,
  },
];

interface IProps {
  next: () => void;
}

const TargetView: React.SFC<IProps> = ({ next }) => {
  function onClickHandler(plugin) {
    if (plugin.disabled) {
      return;
    }

    next();
  }

  return <AbstractPicker plugins={Targets} onClick={onClickHandler} />;
};

const Target = transfersCreateConnect(TargetView);
export default Target;
