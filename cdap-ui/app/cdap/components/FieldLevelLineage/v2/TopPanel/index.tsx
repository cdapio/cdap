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
import withStyles from '@material-ui/core/styles/withStyles';
import TimeRangePicker from 'components/FieldLevelLineage/v2/TimeRangePicker';
import EntityTopPanel from 'components/EntityTopPanel';
import T from 'i18n-react';

const styles = (theme) => {
  return {
    root: {
      height: 60,
      background: theme.palette.white[50],
      display: 'flex',
    },
    entity: {
      flex: '2 42%',
    },
    picker: {
      flex: '1',
      marginTop: '2px',
    },
  };
};

const FllTopPanel = ({ datasetId, classes }) => {
  return (
    <div className={classes.root}>
      <div className={classes.entity}>
        <EntityTopPanel
          breadCrumbAnchorLabel="Results"
          title={datasetId}
          entityType={T.translate(`commons.entity.dataset.singular`)}
          entityIcon="icon-datasets"
          historyBack={true}
          inheritBackground={true}
        />
      </div>
      <span className={classes.picker}>
        <TimeRangePicker />
      </span>
    </div>
  );
};

const StyledTopPanel = withStyles(styles)(FllTopPanel);
export default StyledTopPanel;
