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
import { makeStyles } from '@material-ui/core';
import T from 'i18n-react';
import { UncontrolledTooltip } from 'reactstrap';
import If from 'components/If';
import IconSVG from 'components/IconSVG';
import Heading, { HeadingTypes } from 'components/Heading';
import { IDataModel, IModel } from 'components/DataPrep/store';

const PREFIX = 'features.DataPrep.Directives.MapToTarget.CurrentSelection';

const useStyles = makeStyles({
  selectedItem: {
    display: 'flex',
    flexDirection: 'row',
  },
  selectedItemLabel: {
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    fontWeight: 'bold',
  },
  selectedItemName: {
    flex: 1,
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
  },
  clearIcon: {
    cursor: 'pointer',
  },
});

interface ISelectedItem {
  key: string;
  label: React.ReactNode;
  name: string;
  description?: string;
  onClearClick: () => void;
}

interface ICurrentSelectionProps {
  loading: boolean;
  dataModel?: IDataModel;
  model?: IModel;
  onDataModelClear: () => void;
  onModelClear: () => void;
}

export const CurrentSelection = (props: ICurrentSelectionProps) => {
  const classes = useStyles(undefined);
  const { loading, dataModel, model, onDataModelClear, onModelClear } = props;

  if (!dataModel) {
    return <Heading type={HeadingTypes.h5} label={T.translate(`${PREFIX}.emptySelection`)} />;
  }

  const selection: ISelectedItem[] = [
    {
      key: 'datamodel',
      label: T.translate(`${PREFIX}.dataModelLabel`),
      name: dataModel.name,
      description: dataModel.description,
      onClearClick: onDataModelClear,
    },
  ];
  if (model) {
    selection.push({
      key: 'model',
      label: T.translate(`${PREFIX}.modelLabel`),
      name: model.name,
      description: model.description,
      onClearClick: onModelClear,
    });
  }

  return (
    <div>
      {selection.map((item) => (
        <div
          id={`map-to-target-selected-${item.key}`}
          key={item.key}
          className={classes.selectedItem}
        >
          <span className={classes.selectedItemLabel}>{item.label}:&nbsp;</span>
          <span className={classes.selectedItemName}>{item.name}</span>
          <If condition={!loading}>
            <span className={classes.clearIcon} onClick={item.onClearClick}>
              <IconSVG name="icon-close" />
            </span>
          </If>
          <UncontrolledTooltip
            target={`map-to-target-selected-${item.key}`}
            placement="right-end"
            delay={{ show: 750, hide: 0 }}
          >
            {item.description || item.name}
          </UncontrolledTooltip>
        </div>
      ))}
    </div>
  );
};
