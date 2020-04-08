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
import T from 'i18n-react';
import { makeStyles } from '@material-ui/core';
import classnames from 'classnames';
import { preventPropagation, connectWithStore } from 'services/helpers';
import { setPopoverOffset } from 'components/DataPrep/helper';
import { CurrentSelection } from 'components/DataPrep/Directives/MapToTarget/CurrentSelection';
import { LoadingBar } from 'components/DataPrep/Directives/MapToTarget/LoadingBar';
import { OptionFilter } from 'components/DataPrep/Directives/MapToTarget/OptionFilter';
import { OptionList } from 'components/DataPrep/Directives/MapToTarget/OptionList';
import IconSVG from 'components/IconSVG';
import DataPrepStore, { IDataModel, IModel, IModelField } from 'components/DataPrep/store';
import {
  execute,
  setError,
  loadTargetDataModelStates,
  saveTargetDataModelStates,
  setTargetDataModel,
  setTargetModel,
} from 'components/DataPrep/store/DataPrepActionCreator';

const PREFIX = 'features.DataPrep.Directives.MapToTarget';

const useStyles = makeStyles({
  clearfix: {
    '&::after': {
      display: 'block',
      clear: 'both',
      content: 'none',
    },
  },
  floatRight: {
    float: 'right',
  },
});

interface IMapToTargetProps {
  isOpen: boolean;
  isDisabled: boolean;
  column: string;
  onComplete: () => void;
  close: () => void;
  dataModelList?: IDataModel[];
  targetDataModel?: IDataModel;
  targetModel?: IModel;
}

const MapToTarget = (props: IMapToTargetProps) => {
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

  const [loadingText, setLoadingText] = useState('');
  const [searchText, setSearchText] = useState('');

  useEffect(() => {
    let pending = true;
    setLoadingText(`${PREFIX}.initializingText`);

    const initialize = async () => {
      try {
        await loadTargetDataModelStates();
      } catch (error) {
        setError(error);
      } finally {
        if (pending) {
          setLoadingText('');
        }
      }
    };

    initialize();

    return () => {
      pending = false;
    };
  }, []);

  useEffect(() => {
    if (isOpen && !isDisabled) {
      setPopoverOffset(document.getElementById('map-to-target-directive'));
    }
  });

  const selectTargetDataModel = async (dataModel: IDataModel) => {
    setLoadingText(`${PREFIX}.loadingText`);
    try {
      setTargetModel(null);
      await setTargetDataModel(dataModel);
      setSearchText('');
    } catch (error) {
      setError(error, 'Could not set target data model');
    } finally {
      setLoadingText('');
    }
  };

  const selectTargetModel = (model: IModel) => {
    setTargetModel(model);
    setSearchText('');
  };

  const applyDirective = async (field: IModelField) => {
    setLoadingText(`${PREFIX}.executingDirectiveText`);
    try {
      await saveTargetDataModelStates();
    } catch (error) {
      setError(error, 'Could not save target data model states');
      setLoadingText('');
      return;
    }

    executeDirective(field);
  };

  const executeDirective = (field: IModelField) => {
    const directive =
      'data-model-map-column ' +
      `'${targetDataModel.url}' '${targetDataModel.id}' ${targetDataModel.revision} ` +
      `'${targetModel.id}' '${field.id}' :${column}`;

    execute([directive], false, true).subscribe(
      () => {
        close();
        onComplete();
      },
      (error) => {
        setError(error, 'Error executing Map to Target directive');
      }
    );
  };

  const renderDetail = () => {
    if (!isOpen || isDisabled) {
      return null;
    }

    return (
      <div className="second-level-popover" onClick={preventPropagation}>
        <CurrentSelection
          loading={!!loadingText}
          dataModel={targetDataModel}
          model={targetModel}
          onDataModelClear={() => selectTargetDataModel(null)}
          onModelClear={() => selectTargetModel(null)}
        />
        <LoadingBar loadingText={loadingText} />
        <OptionFilter
          loading={!!loadingText}
          searchText={searchText}
          dataModel={targetDataModel}
          model={targetModel}
          onSearchTextChange={(text) => setSearchText(text)}
        />
        <OptionList
          loading={!!loadingText}
          searchText={searchText}
          dataModelList={dataModelList}
          dataModel={targetDataModel}
          model={targetModel}
          onDataModelSelect={selectTargetDataModel}
          onModelSelect={selectTargetModel}
          onFieldSelect={applyDirective}
        />
      </div>
    );
  };

  return (
    <div
      id="map-to-target-directive"
      className={classnames(classes.clearfix, 'action-item', {
        active: isOpen && !isDisabled,
        disabled: isDisabled,
      })}
    >
      <span>{T.translate(`${PREFIX}.title`)}</span>
      <span className={classes.floatRight}>
        <IconSVG name="icon-caret-right" />
      </span>
      {renderDetail()}
    </div>
  );
};

const mapStateToProps = (state) => {
  const { dataModelList, targetDataModel, targetModel } = state.dataprep;
  return {
    dataModelList,
    targetDataModel,
    targetModel,
  };
};

export default connectWithStore(DataPrepStore, MapToTarget, mapStateToProps);
