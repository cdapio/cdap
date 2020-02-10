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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import If from 'components/If';
import Mappings from './Mappings';

const styles = (theme): StyleRules => {
  return {
    gridWrapper: {
      height: '100%',
      '& .grid.grid-container.grid-compact': {
        maxHeight: '100%',

        '& .grid-row': {
          gridTemplateColumns: '2fr 1fr 1fr 1fr 1fr',
        },
      },
    },
    mappingButton: {
      color: theme.palette.grey[200],
      cursor: 'pointer',
      '&:hover': {
        textDecoration: 'underline',
        color: theme.palette.blue[200],
      },
    },
  };
};

interface ITablesAssessmentProps extends ICreateContext, WithStyles<typeof styles> {
  tables: any;
}

const TablesAssessmentView: React.FC<ITablesAssessmentProps> = ({ classes, tables }) => {
  const [openTable, setOpenTable] = React.useState(null);

  if (tables.length === 0) {
    return (
      <div>
        <h5>No tables</h5>
      </div>
    );
  }

  return (
    <React.Fragment>
      <div className={`grid-wrapper ${classes.gridWrapper}`}>
        <div className="grid grid-container grid-compact">
          <div className="grid-header">
            <div className="grid-row">
              <div>Name</div>
              <div>Number of columns</div>
              <div>Partially supported</div>
              <div>Not supported</div>
              <div />
            </div>
          </div>

          <div className="grid-body">
            {tables.map((row) => {
              return (
                <div key={`${row.database}-${row.table}`} className="grid-row">
                  <div>{row.table}</div>
                  <div>{row.numColumns}</div>
                  <div>{row.numColumnsPartiallySupported}</div>
                  <div>{row.numColumnsNotSupported}</div>
                  <div>
                    <span className={classes.mappingButton} onClick={() => setOpenTable(row)}>
                      View mappings
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <If condition={openTable}>
        <Mappings tableInfo={openTable} onClose={() => setOpenTable(null)} />
      </If>
    </React.Fragment>
  );
};

const StyledTablesAssessment = withStyles(styles)(TablesAssessmentView);
const TablesAssessment = createContextConnect(StyledTablesAssessment);
export default TablesAssessment;
