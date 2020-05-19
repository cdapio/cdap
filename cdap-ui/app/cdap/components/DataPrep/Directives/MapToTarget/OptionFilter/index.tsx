/*
 * Copyright © 2020 Cask Data, Inc.
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
import T from 'i18n-react';
import { makeStyles, Theme } from '@material-ui/core';
import Input from '@material-ui/core/Input';
import { IDataModel, IModel } from 'components/DataPrep/store';

const PREFIX = 'features.DataPrep.Directives.MapToTarget.OptionFilter';

const useStyles = makeStyles((theme: Theme) => ({
  searchField: {
    width: '100%',
    marginBottom: theme.spacing(0.5),
  },
}));

interface IOptionFilterProps {
  loading: boolean;
  searchText: string;
  dataModel?: IDataModel;
  model?: IModel;
  onSearchTextChange: (text: string) => void;
}

export const OptionFilter = (props: IOptionFilterProps) => {
  const classes = useStyles(undefined);
  const { loading, searchText, dataModel, model, onSearchTextChange } = props;

  if (loading || !dataModel) {
    return null;
  }

  const placeholder = T.translate(
    model ? `${PREFIX}.fieldPlaceholder` : `${PREFIX}.modelPlaceholder`
  );

  return (
    <Input
      autoFocus={true}
      type="text"
      className={classes.searchField}
      value={searchText}
      placeholder={`${placeholder}`}
      onChange={(event) => onSearchTextChange(event.target.value)}
    />
  );
};
