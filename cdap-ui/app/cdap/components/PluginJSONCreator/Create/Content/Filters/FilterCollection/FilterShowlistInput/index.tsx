/*
 * Copyright © 2018 Cask Data, Inc.
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

import { IconButton, WithStyles } from '@material-ui/core';
import { StyleRules, withStyles } from '@material-ui/core/styles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import classnames from 'classnames';
import { IPropertyShowConfig } from 'components/ConfigurationGroup/types';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { SHOW_TYPE_VALUES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { ICreateContext, IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (): StyleRules => {
  return {
    showConfigCollection: {
      display: 'grid',
      gridAutoFlow: 'column',
      width: '100%',
    },
    showConfigInput: {
      gridRow: '1',
      width: '100%',
    },
    showConfigNameInput: {
      gridColumnStart: '1',
      gridColumnEnd: '8',
    },
    showConfigTypeInput: {
      gridColumnStart: '9',
      gridColumnEnd: '15',
    },
    showAddDeleteButtonInput: {
      gridColumnStart: '19',
      gridColumnEnd: '20',
    },
    showAddDeleteButton: {
      float: 'right',
    },
  };
};

interface IFilterShowlistInputProps extends WithStyles<typeof styles>, ICreateContext {
  filterID: string;
}

const FilterShowlistInputView: React.FC<IFilterShowlistInputProps> = ({
  classes,
  filterID,
  filterToShowList,
  setFilterToShowList,
  showToInfo,
  setShowToInfo,
  widgetToInfo,
}) => {
  const allWidgetNames = widgetToInfo
    ? Object.values(widgetToInfo)
        .map((widgetInfo: IWidgetInfo) => widgetInfo.name)
        .filter((widgetName) => widgetName !== undefined && widgetName !== null)
    : [];

  function setShowProperty(showID: string, property: string) {
    return (val) => {
      setShowToInfo((prevObjs) => ({
        ...prevObjs,
        [showID]: { ...prevObjs[showID], [property]: val },
      }));
    };
  }

  function addShowToFilter(filterObjID: string, index?: number) {
    const newShowID = 'Show_' + uuidV4();

    setShowToInfo({
      ...showToInfo,
      [newShowID]: {
        name: '',
      } as IPropertyShowConfig,
    });

    if (index === undefined) {
      setFilterToShowList({
        ...filterToShowList,
        [filterObjID]: [...filterToShowList[filterObjID], newShowID],
      });
    } else {
      const showList = filterToShowList[filterObjID];

      if (showList.length === 0) {
        showList.splice(0, 0, newShowID);
      } else {
        showList.splice(index + 1, 0, newShowID);
      }

      setFilterToShowList({
        ...filterToShowList,
        [filterObjID]: showList,
      });
    }
  }

  function deleteShowFromFilter(filterObjID: string, index: number) {
    const showList = filterToShowList[filterObjID];

    const showToDelete = showList[index];

    showList.splice(index, 1);

    setFilterToShowList({
      ...filterToShowList,
      [filterObjID]: showList,
    });

    const { [showToDelete]: tmp, ...restShowToInfo } = showToInfo;
    setShowToInfo(restShowToInfo);
  }

  return (
    <If condition={filterToShowList[filterID] !== undefined}>
      <Heading type={HeadingTypes.h6} label="Add widgets to configure" />
      {filterToShowList[filterID].map((showID: string, showIndex: number) => {
        return (
          <If condition={showToInfo[showID]}>
            <div className={classes.showConfigCollection}>
              <div className={classnames(classes.showConfigInput, classes.showConfigNameInput)}>
                <PluginInput
                  widgetType={'select'}
                  value={showToInfo[showID].name}
                  setValue={setShowProperty(showID, 'name')}
                  label={'name'}
                  options={allWidgetNames}
                  required={true}
                />
              </div>
              <div className={classnames(classes.showConfigInput, classes.showConfigTypeInput)}>
                <PluginInput
                  widgetType={'select'}
                  value={showToInfo[showID].type}
                  setValue={setShowProperty(showID, 'type')}
                  options={SHOW_TYPE_VALUES}
                  label={'type'}
                  required={false}
                />
              </div>
              <div
                className={classnames(classes.showConfigInput, classes.showAddDeleteButtonInput)}
              >
                <div className={classes.showAddDeleteButton}>
                  <IconButton
                    onClick={() => addShowToFilter(filterID, showIndex)}
                    data-cy="add-row"
                  >
                    <AddIcon fontSize="small" />
                  </IconButton>
                  <IconButton
                    onClick={() => deleteShowFromFilter(filterID, showIndex)}
                    color="secondary"
                    data-cy="remove-row"
                  >
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </div>
              </div>
            </div>
          </If>
        );
      })}
    </If>
  );
};

const FilterShowlistInput = withStyles(styles)(FilterShowlistInputView);
export default FilterShowlistInput;
