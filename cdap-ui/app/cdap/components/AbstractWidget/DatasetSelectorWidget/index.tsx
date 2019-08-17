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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { MyDatasetApi } from 'api/dataset';
import { getCurrentNamespace } from 'services/NamespaceStore';
import If from 'components/If';
import classnames from 'classnames';
import ee from 'event-emitter';
import { objectQuery } from 'services/helpers';
import ThemeWrapper from 'components/ThemeWrapper';
import ClickAwayListener from '@material-ui/core/ClickAwayListener';
import { KEY_CODE } from 'services/global-constants';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import { IWidgetProps } from 'components/AbstractWidget';
import InputBase from '@material-ui/core/InputBase';

const styles = (theme): StyleRules => {
  return {
    autocompleteContainer: {
      position: 'relative',
    },
    autocomplete: {
      position: 'absolute',
      top: 0,
      left: 0,
      right: 0,
      backgroundColor: 'white',
      border: `1px solid ${theme.palette.grey[400]}`,
      zIndex: 101,
      padding: '5px 0',
      boxShadow: `0 3px 10px 0 ${theme.palette.grey[200]}`,
    },
    autocompleteRow: {
      lineHeight: '1.5rem',
      padding: '8px 10px',
      cursor: 'pointer',
      '&:not(:last-child)': {
        borderBottom: `1px solid ${theme.palette.grey[400]}`,
      },
      '&:hover': {
        backgroundColor: theme.palette.grey[600],
      },
    },
    highlight: {
      backgroundColor: `${theme.palette.blue[100]} !important`,
      color: 'white',
    },
  };
};

interface IDatasetSelectorWidgetProps {
  placeholder?: string;
}

interface IDatasetSelectorProps
  extends IWidgetProps<IDatasetSelectorWidgetProps>,
    WithStyles<typeof styles> {}

const RESULT_LIMIT = 5;

export const DatasetSelectedEvent = 'dataset.selected';

const DatasetSelectorView: React.SFC<IDatasetSelectorProps> = ({
  value,
  onChange,
  classes,
  widgetProps,
  disabled,
}) => {
  const [datasets, setDatasets] = React.useState([]);
  const [showAutocomplete, setShowAutoComplete] = React.useState(false);
  const [selection, setSelection] = React.useState(0);
  const eventEmitter = ee(ee);
  const placeholder = objectQuery(widgetProps, 'placeholder');

  React.useEffect(() => {
    MyDatasetApi.list({ namespace: getCurrentNamespace() }).subscribe(setDatasets);
  }, []);

  function handleChange(e) {
    setShowAutoComplete(true);
    onChange(e.target.value);
  }

  function getMatch() {
    if (!value || value.toString().length === 0) {
      return datasets.slice(0, RESULT_LIMIT);
    }
    return datasets
      .filter((dataset) => {
        const datasetName = dataset.name.toLowerCase();
        const inputValue = value.toString().toLowerCase();
        return datasetName.indexOf(inputValue) !== -1;
      })
      .slice(0, RESULT_LIMIT);
  }

  function handleRowClick(dataset) {
    onChange(dataset.name);
    const schema = objectQuery(dataset, 'properties', 'schema');
    if (!schema) {
      eventEmitter.emit('schema.clear');
    } else {
      eventEmitter.emit(DatasetSelectedEvent, schema, null, true, dataset.name);
    }
    setShowAutoComplete(false);
  }

  const matches = getMatch();

  function changeSelection(newSelection) {
    let finalSelection = newSelection;
    if (newSelection < 0) {
      finalSelection = matches.length - 1;
    } else if (newSelection > matches.length - 1) {
      finalSelection = 0;
    }

    setSelection(finalSelection);
  }

  function handleKeyDown(e) {
    switch (e.nativeEvent.keyCode) {
      case KEY_CODE.Up:
        e.preventDefault();
        changeSelection(selection - 1);
        return;
      case KEY_CODE.Down:
        changeSelection(selection + 1);
        return;
    }
  }

  function handleKeyPress(e) {
    if (e.nativeEvent.keyCode === KEY_CODE.Enter && matches[selection]) {
      handleRowClick(matches[selection]);
      return;
    }

    setSelection(0);
  }

  function handleBlur() {
    if (!showAutocomplete) {
      return;
    }

    const exactMatch = matches.filter((dataset) => dataset.name === value);

    if (exactMatch.length > 0) {
      handleRowClick(exactMatch[0]);
    } else {
      eventEmitter.emit(DatasetSelectedEvent, '', null, false);
      setShowAutoComplete(false);
    }
  }

  return (
    <ClickAwayListener onClickAway={handleBlur}>
      <div>
        <InputBase
          value={value}
          onChange={handleChange}
          fullWidth
          onKeyDown={handleKeyDown}
          onKeyPress={handleKeyPress}
          placeholder={placeholder}
          disabled={disabled}
          onFocus={() => setShowAutoComplete(true)}
        />
        <div className={classes.autocompleteContainer}>
          <If condition={showAutocomplete && matches.length > 0}>
            <div className={classes.autocomplete}>
              {matches.map((dataset, i) => {
                return (
                  <div
                    className={classnames(classes.autocompleteRow, {
                      [classes.highlight]: i === selection,
                    })}
                    onClick={handleRowClick.bind(null, dataset)}
                    key={dataset.name}
                  >
                    {dataset.name}
                  </div>
                );
              })}
            </div>
          </If>
        </div>
      </div>
    </ClickAwayListener>
  );
};

const StyledDatasetSelector = withStyles(styles)(DatasetSelectorView);

function DatasetSelector(props) {
  return (
    <ThemeWrapper>
      <StyledDatasetSelector {...props} />
    </ThemeWrapper>
  );
}
(DatasetSelector as any).propTypes = WIDGET_PROPTYPES;

export default DatasetSelector;
