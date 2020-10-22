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

import * as React from 'react';

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import { List } from 'immutable';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { SHOW_TYPE_VALUES } from 'components/PluginJSONCreator/constants';
import ShowActionButtons from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterShowlistInput/ShowPropertyRow/ShowActionButtons';
import classnames from 'classnames';
import { useWidgetState } from 'components/PluginJSONCreator/Create';

const styles = (): StyleRules => {
  return {
    showRow: {
      display: 'grid',
      gridAutoFlow: 'column',
      width: '100%',
    },
    ShowPropertyRow: {
      gridRow: '1',
      width: '100%',
      marginTop: '5px',
      marginBottom: '5px',
    },
    showNameInput: {
      gridColumnStart: '1',
      gridColumnEnd: '8',
    },
    showTypeInput: {
      gridColumnStart: '9',
      gridColumnEnd: '15',
    },
    showAddDeleteContainer: {
      gridColumnStart: '19',
      gridColumnEnd: '20',
    },
  };
};

interface IShowPropertyRowProps extends WithStyles<typeof styles> {
  showName: string;
  showType: string;
  setShowName: (newVal: string) => void;
  setShowType: (newVal: string) => void;
  addShowToFilter: () => void;
  deleteShowFromFilter: () => void;
}

const ShowPropertyRowView: React.FC<IShowPropertyRowProps> = ({
  classes,
  showName,
  showType,
  setShowName,
  setShowType,
  addShowToFilter,
  deleteShowFromFilter,
}) => {
  const { widgetInfo } = useWidgetState();

  const allWidgetNames = widgetInfo
    ? widgetInfo
        .valueSeq()
        .map((info) => info.get('name'))
        .filter((widgetName) => widgetName && widgetName.length > 0)
    : List([]);

  return (
    <div className={classes.showRow}>
      <div className={classnames(classes.ShowPropertyRow, classes.showNameInput)}>
        <PluginInput
          widgetType={'select'}
          value={showName}
          onChange={setShowName}
          options={allWidgetNames}
          label={'Name'}
          required={true}
        />
      </div>
      <div className={classnames(classes.ShowPropertyRow, classes.showTypeInput)}>
        <PluginInput
          widgetType={'select'}
          value={showType}
          onChange={setShowType}
          options={SHOW_TYPE_VALUES}
          label={'Type'}
          required={false}
        />
      </div>
      <div className={classnames(classes.ShowPropertyRow, classes.showAddDeleteContainer)}>
        <ShowActionButtons
          addShowToFilter={addShowToFilter}
          deleteShowFromFilter={deleteShowFromFilter}
        />
      </div>
    </div>
  );
};

const ShowPropertyRow = withStyles(styles)(ShowPropertyRowView);
export default ShowPropertyRow;
