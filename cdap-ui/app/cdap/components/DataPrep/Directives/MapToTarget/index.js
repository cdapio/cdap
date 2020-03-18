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

import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import { makeStyles } from '@material-ui/core';
import Input from '@material-ui/core/Input';
import LinearProgress from '@material-ui/core/LinearProgress';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import { UncontrolledTooltip } from 'reactstrap';
import classnames from 'classnames';
import { preventPropagation, connectWithStore } from 'services/helpers';
import { setPopoverOffset } from 'components/DataPrep/helper';
import If from 'components/If';
import DataPrepStore from 'components/DataPrep/store';
import {
  execute,
  setError,
  loadTargetDataModelStates,
  saveTargetDataModelStates,
  setTargetDataModel,
  setTargetModel
} from 'components/DataPrep/store/DataPrepActionCreator';

const useStyles = makeStyles(theme => ({
  secondLevelPopover: {
    width: '300px !important',
  },
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
  unselectIcon: {
    cursor: 'pointer',
    padding: theme.spacing(0.75),
    margin: '0 !important',
    '&:hover': {
      fontWeight: 'bold',
    },
  },
  optionSearch: {
    width: '100%',
    marginBottom: theme.spacing(0.5),
  },
  targetOptionList: {
    overflowX: 'hidden',
    overflowY: 'auto',
    maxHeight: '400px',
  },
  targetOption: {
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis',
    overflow: 'hidden',
  },
  highlight: {
    color: theme.palette.primary.contrastText,
    backgroundColor: theme.palette.primary.dark,
  },
}));

const PREFIX = 'features.DataPrep.Directives.MapToTarget';

const MapToTarget = (props) => {
  const classes = useStyles(undefined);
  const {
    isOpen,
    isDisabled,
    column,
    onComplete,
    close,
    dataModelList,
    targetDataModel,
    targetModel,
  } = props;
  const [loading, setLoading] = useState('');
  const [searchText, setSearchText] = useState('');

  useEffect(() => {
    let pending = true;
    setLoading(`${PREFIX}.initializingText`);
    (async () => {
      try {
        await loadTargetDataModelStates();
      } catch (error) {
        setError(error);
      } finally {
        if (pending) {
          setLoading('');
        }
      }
    })();
    return () => {
      pending = false;
    }
  }, []);

  useEffect(() => {
    if (isOpen && !isDisabled) {
      setPopoverOffset(document.getElementById('map-to-target-directive'));
    }
  });

  const applySearch = (options) => {
    const searchTextUpper = searchText.trim().toUpperCase();
    if (searchTextUpper) {
      return options.filter((option) => option.name.toUpperCase().indexOf(searchTextUpper) >= 0);
    }
    return options;
  };

  const highlightText = (text) => {
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
        <span className={classes.highlight}>{highlightedText}</span>
        {trailingText}
      </span>
    );
  };

  const resetTargetOptionsScroll = () => {
    const element = document.querySelector('.' + classes.targetOptionList);
    if (element) {
      element.scrollTop = 0;
    }
  };

  const selectTargetDataModel = async (dataModel) => {
    setLoading(`${PREFIX}.loadingText`);
    try {
      setTargetModel(null);
      await setTargetDataModel(dataModel);
      setSearchText('');
      resetTargetOptionsScroll();
    } catch (error) {
      setError(error, 'Could not set target data model');
    } finally {
      setLoading('');
    }
  };

  const selectTargetModel = (model) => {
    setTargetModel(model);
    setSearchText('');
    resetTargetOptionsScroll();
  };

  const applyDirective = async (field) => {
    setLoading(`${PREFIX}.executingDirectiveText`);
    try {
      await saveTargetDataModelStates();

      const directive = 'data-model-map-column ' +
        `'${targetDataModel.url}' '${targetDataModel.id}' ${targetDataModel.revision} ` +
        `'${targetModel.id}' '${field.id}' :${column}`;

      await execute([directive], false, true).toPromise();

      close();
      onComplete();
    } catch (error) {
      setError(error, 'Error executing Map to Target directive');
    } finally {
      setLoading('');
    }
  };

  const renderHeader = (selection) => {
    return (
      <div>
        {selection.length === 0 ? <h5>{T.translate(`${PREFIX}.dataModelPlaceholder`)}</h5> : null}
        {selection.map(item => (
          <div id={`map-to-target-selected-${item.key}`} key={item.key} className={classes.selectedItem}>
            <span className={classes.selectedItemLabel}>{item.label}:&nbsp;</span>
            <span className={classes.selectedItemName}>{item.name}</span>
            <span className={classnames('fa fa-times', classes.unselectIcon)} onClick={item.unselectFn} />
            <If condition={item.description}>
              <UncontrolledTooltip
                target={`map-to-target-selected-${item.key}`}
                placement='right-end'
                delay={{ show: 750, hide: 0 }}
              >
                {item.description}
              </UncontrolledTooltip>
            </If>
          </div>
        ))}
      </div>
    );
  };

  const renderLoading = () => {
    if (!loading) {
      return <hr />;
    }
    return (
      <div>
        <LinearProgress />
        <div>{T.translate(loading)}</div>
      </div>
    );
  };

  const renderFilter = (placeholder) => {
    if (loading || !targetDataModel) {
      return null;
    }
    return (
      <Input
        autoFocus={true}
        type='text'
        className={classes.optionSearch}
        value={searchText}
        placeholder={placeholder}
        onChange={(event) => setSearchText(event.target.value)}
      />
    );
  };

  const renderOptions = (options, selectFn) => {
    if (loading) {
      return null;
    }
    return (
      <List dense={true} disablePadding={true} className={classes.targetOptionList} hidden={loading}>
        {options.map((option, index) => (
          <div key={option.id}>
            <ListItem
              button={true}
              id={`map-to-target-option-${index}`}
              onClick={() => selectFn(option)}
            >
              <ListItemText
                className={classes.targetOption}
                primary={highlightText(option.name)}
              />
            </ListItem>
            <If condition={option.description}>
              <UncontrolledTooltip
                target={`map-to-target-option-${index}`}
                modifiers={{
                  preventOverflow: {
                    boundariesElement: 'window'
                  }
                }}
                placement='right'
                delay={{ show: 500, hide: 0 }}
              >
                {option.description}
              </UncontrolledTooltip>
            </If>
          </div>
        ))}
      </List>
    );
  };

  const renderDetail = () => {
    if (!isOpen || isDisabled) {
      return null;
    }

    let options, selectFn;
    let filterPlaceholder;
    const selection = [];

    if (targetDataModel) {
      selection.push(
        {
          key: 'datamodel',
          label: T.translate(`${PREFIX}.dataModelLabel`),
          unselectFn: () => (async() => await selectTargetDataModel(null))(),
          ...targetDataModel,
        }
      );
      if (targetModel) {
        selection.push(
          {
            key: 'model',
            label: T.translate(`${PREFIX}.modelLabel`),
            unselectFn: () => selectTargetModel(null),
            ...targetModel,
          }
        );
        filterPlaceholder = T.translate(`${PREFIX}.fieldFilterPlaceholder`);
        options = applySearch(targetModel.fields || []);
        selectFn = (field) => (async () => await applyDirective(field))();
      } else {
        filterPlaceholder = T.translate(`${PREFIX}.modelFilterPlaceholder`);
        options = applySearch(targetDataModel.models || []);
        selectFn = (model) => selectTargetModel(model);
      }
    } else {
      options = dataModelList || [];
      selectFn = (dataModel) => (async () => await selectTargetDataModel(dataModel))();
    }

    return (
      <div className={classnames('second-level-popover', classes.secondLevelPopover)} onClick={preventPropagation}>
        {renderHeader(selection)}
        {renderLoading()}
        {renderFilter(filterPlaceholder)}
        {renderOptions(options, selectFn)}
      </div>
    );
  };

  return (
    <div
      id='map-to-target-directive'
      className={classnames('map-to-target-directive clearfix action-item', {
        active: isOpen && !isDisabled,
        disabled: isDisabled,
      })}
    >
      <span>{T.translate(`${PREFIX}.title`)}</span>
      <span className='float-right'>
          <span className='fa fa-caret-right' />
        </span>
      {renderDetail()}
    </div>
  );
};

MapToTarget.propTypes = {
  isOpen: PropTypes.bool,
  isDisabled: PropTypes.bool,
  column: PropTypes.string,
  onComplete: PropTypes.func,
  close: PropTypes.func,
  dataModelList: PropTypes.array,
  targetDataModel: PropTypes.object,
  targetModel: PropTypes.object,
};

const mapStateToProps = state => {
  const { dataModelList, targetDataModel, targetModel } = state.dataprep;
  return {
    dataModelList,
    targetDataModel,
    targetModel,
  };
};

export default connectWithStore(DataPrepStore, MapToTarget, mapStateToProps);
