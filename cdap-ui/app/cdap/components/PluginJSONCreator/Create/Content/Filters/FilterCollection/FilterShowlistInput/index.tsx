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

import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import classnames from 'classnames';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { SHOW_TYPE_VALUES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import { List, Map } from 'immutable';
import isNil from 'lodash/isNil';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (): StyleRules => {
  return {
    showConfigCollection: {
      display: 'grid',
      gridAutoFlow: 'column',
      width: '100%',
      marginTop: '10px',
      marginBottom: '10px',
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
  widgetInfo,
}) => {
  const allWidgetNames = widgetInfo
    ? widgetInfo
        .valueSeq()
        .map((info) => info.get('name'))
        .filter((widgetName) => !isNil(widgetName))
    : List([]);

  function setShowProperty(showID: string, property: string) {
    return (val) => {
      setShowToInfo(showToInfo.setIn([showID, property], val));
    };
  }

  function addShowToFilter(filterObjID: string, index: number) {
    const newShowID = 'Show_' + uuidV4();

    setShowToInfo(
      showToInfo.set(
        newShowID,
        Map({
          name: '',
          type: '',
        })
      )
    );

    const showlist = filterToShowList.get(filterObjID);

    let newShowlist;
    if (showlist.isEmpty()) {
      newShowlist = showlist.insert(0, newShowID);
    } else {
      newShowlist = showlist.insert(index + 1, newShowID);
    }

    setFilterToShowList(filterToShowList.set(filterObjID, newShowlist));
  }

  function deleteShowFromFilter(filterObjID: string, index: number) {
    const showlist = filterToShowList.get(filterObjID);
    const showToDelete = showlist.get(index);

    const newShowlist = showlist.remove(index);
    setFilterToShowList(filterToShowList.set(filterObjID, newShowlist));

    const newShowToInfo = showToInfo.delete(showToDelete);
    setShowToInfo(newShowToInfo);
  }

  return (
    <If condition={filterToShowList.has(filterID)}>
      <Heading type={HeadingTypes.h6} label="Add widgets to configure" />
      {filterToShowList.get(filterID).map((showID: string, showIndex: number) => {
        if (!showToInfo.has(showID)) {
          return null;
        }
        return (
          <div key={showID} className={classes.showConfigCollection}>
            <div className={classnames(classes.showConfigInput, classes.showConfigNameInput)}>
              <PluginInput
                widgetType={'select'}
                value={showToInfo.get(showID).get('name')}
                onChange={setShowProperty(showID, 'name')}
                label={'name'}
                options={allWidgetNames}
                required={true}
              />
            </div>
            <div className={classnames(classes.showConfigInput, classes.showConfigTypeInput)}>
              <PluginInput
                widgetType={'select'}
                value={showToInfo.get(showID).get('type')}
                onChange={setShowProperty(showID, 'type')}
                options={SHOW_TYPE_VALUES}
                label={'type'}
                required={false}
              />
            </div>
            <div className={classnames(classes.showConfigInput, classes.showAddDeleteButtonInput)}>
              <div className={classes.showAddDeleteButton}>
                <IconButton onClick={() => addShowToFilter(filterID, showIndex)} data-cy="add-row">
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
        );
      })}
    </If>
  );
};

const FilterShowlistInput = withStyles(styles)(FilterShowlistInputView);
export default FilterShowlistInput;
