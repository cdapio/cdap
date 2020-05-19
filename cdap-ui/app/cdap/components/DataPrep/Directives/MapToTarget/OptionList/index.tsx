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

import React, { useEffect } from 'react';
import { makeStyles, Theme } from '@material-ui/core';
import { UncontrolledTooltip } from 'reactstrap';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import { IDataModel, IModel, IModelField } from 'components/DataPrep/store';

const useStyles = makeStyles((theme: Theme) => ({
  optionList: {
    overflowX: 'hidden',
    overflowY: 'auto',
    maxHeight: '400px',
    marginLeft: '-10px',
    marginRight: '-10px',
  },
  optionItemDense: {
    paddingTop: 0,
    paddingBottom: 0,
  },
  optionItemGutters: {
    paddingLeft: theme.spacing(1.25),
    paddingRight: theme.spacing(1.25),
  },
  optionItemText: {
    marginTop: '1px',
    marginBottom: '2px',
  },
  optionText: {
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    fontSize: '13px',
  },
  highlightedText: {
    color: theme.palette.primary.contrastText,
    backgroundColor: theme.palette.primary.dark,
  },
}));

type Option = IDataModel | IModel | IModelField;

interface IOptionListProps {
  loading: boolean;
  searchText: string;
  dataModelList?: IDataModel[];
  dataModel?: IDataModel;
  model?: IModel;
  onDataModelSelect: (dataModel: IDataModel) => void;
  onModelSelect: (model: IModel) => void;
  onFieldSelect: (field: IModelField) => void;
}

export const OptionList = (props: IOptionListProps) => {
  const classes = useStyles(undefined);

  const {
    loading,
    searchText,
    dataModelList,
    dataModel,
    model,
    onDataModelSelect,
    onModelSelect,
    onFieldSelect,
  } = props;

  useEffect(() => {
    const element = document.querySelector('.' + classes.optionList);
    if (element) {
      element.scrollTop = 0;
    }
  }, [dataModel, model]);

  if (loading) {
    return null;
  }

  let options: Option[];
  let onOptionClick: (option: Option) => void;
  if (model) {
    options = model.fields;
    onOptionClick = onFieldSelect;
  } else if (dataModel) {
    options = dataModel.models;
    onOptionClick = onModelSelect;
  } else {
    options = dataModelList || [];
    onOptionClick = onDataModelSelect;
  }

  const applyFilter = () => {
    const searchTextUpper = searchText.trim().toUpperCase();
    if (searchTextUpper) {
      return options.filter((option) => option.name.toUpperCase().indexOf(searchTextUpper) >= 0);
    }
    return options;
  };

  const highlightText = (text: string): string | React.ReactNode => {
    const searchTextUpper = searchText.trim().toUpperCase();
    if (!searchTextUpper) {
      return text;
    }
    const index = text.toUpperCase().indexOf(searchTextUpper);
    if (index < 0) {
      return text;
    }
    const leadingText = text.substring(0, index);
    const highlightedText = text.substring(index, index + searchTextUpper.length);
    const trailingText = text.substring(index + searchTextUpper.length);
    return (
      <span>
        {leadingText}
        <span className={classes.highlightedText}>{highlightedText}</span>
        {trailingText}
      </span>
    );
  };

  return (
    <List dense={true} disablePadding={true} className={classes.optionList}>
      {applyFilter().map((option) => (
        <ListItem
          classes={{
            dense: classes.optionItemDense,
            gutters: classes.optionItemGutters,
          }}
          button={true}
          key={option.id}
          id={`map-to-target-option-${option.uuid}`}
          onClick={() => onOptionClick(option)}
        >
          <ListItemText
            classes={{
              root: classes.optionItemText,
            }}
            disableTypography={true}
            className={classes.optionText}
            primary={highlightText(option.name)}
          />
          <UncontrolledTooltip
            target={`map-to-target-option-${option.uuid}`}
            modifiers={{
              preventOverflow: {
                boundariesElement: 'window',
              },
            }}
            placement="right"
            delay={{ show: 500, hide: 0 }}
          >
            {option.description || option.name}
          </UncontrolledTooltip>
        </ListItem>
      ))}
    </List>
  );
};
