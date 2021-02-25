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
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import Heading, { HeadingTypes } from 'components/Heading';
import { objectQuery } from 'services/helpers';
import Supported, {
  SUPPORT,
} from 'components/Replicator/Create/Content/Assessment/TablesAssessment/Mappings/Supported';
import sortBy from 'lodash/sortBy';
import { generateTableKey } from 'components/Replicator/utilities';
import { List, Map } from 'immutable';
import { IColumnsList, ITableInfo } from 'components/Replicator/types';

const styles = (theme): StyleRules => {
  const headerHeight = '60px';

  return {
    root: {
      position: 'absolute',
      top: '50px',
      bottom: '54px',
      left: 0,
      right: 0,
      background: theme.palette.white[50],
      zIndex: 10,
    },
    header: {
      display: 'grid',
      gridTemplateColumns: '75px 1fr',
      height: headerHeight,
      backgroundColor: theme.palette.grey[700],
      borderBottom: `1px solid ${theme.palette.grey[400]}`,
      alignContent: 'center',
      alignItems: 'center',
      '& > div': {
        padding: '0 15px',
      },
    },
    headerPluginTypes: {
      fontWeight: 500,
      color: theme.palette.grey[100],

      '& > span': {
        marginRight: '5px',
      },
    },
    backButton: {
      color: theme.palette.blue[100],
      cursor: 'pointer',
      '& > span:first-child': {
        marginRight: '5px',
      },

      '&:hover > span:last-child': {
        textDecoration: 'underline',
      },
    },
    separator: {
      marginLeft: '15px',
    },
    columnAction: {
      color: theme.palette.blue[100],
      cursor: 'pointer',

      '&:hover': {
        textDecoration: 'underline',
      },
    },
    actionsSeparator: {
      marginLeft: '5px',
      marginRight: '5px',
    },
    mappings: {
      padding: '10px 25px',
      height: `calc(100% - ${headerHeight})`,

      '& .grid-wrapper': {
        height: '100%',

        '& .grid.grid-container.grid-compact': {
          maxHeight: '100%',

          '& .section-heading': {
            fontWeight: 'bold',
            lineHeight: 1.2,
          },

          '& .grid-header': {
            '& .grid-row': {
              minHeight: 0,

              '&:first-child': {
                borderBottom: 0,

                '& > div': {
                  paddingTop: '5px',
                },
              },

              '&:last-child > div': {
                paddingBottom: '5px',
              },

              '& > div': {
                paddingTop: 0,
                paddingBottom: 0,
              },
            },
          },

          '& .grid-row': {
            gridTemplateColumns: '2fr 2fr 125px 25px 125px 3fr 150px 120px',

            '& > div:first-child:not($headerDataTypes)': {
              paddingLeft: '25px',
            },
          },
        },
      },
    },
    headerColumnName: {
      gridColumn: '1 / span 2',
    },
    headerDataTypes: {
      gridColumn: '3 / span 3',
    },
  };
};

interface IMappingsProps extends ICreateContext, WithStyles<typeof styles> {
  tableInfo: ITableInfo;
  onClose: (rerunAssessment) => void;
}

function getPluginDisplayName(pluginInfo, pluginWidget) {
  const displayName = objectQuery(pluginWidget, 'display-name') || objectQuery(pluginInfo, 'name');
  return displayName;
}

const MappingsView: React.FC<IMappingsProps> = ({
  classes,
  tableInfo,
  onClose,
  draftId,
  name,
  sourcePluginInfo,
  sourcePluginWidget,
  targetPluginInfo,
  targetPluginWidget,
  columns,
  setColumns,
  saveDraft,
}) => {
  const [assessmentColumns, setAssessmentColumns] = React.useState([]);
  const [error, setError] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [rerunAssessment, setRerunAssessment] = React.useState(false);

  function assessTable(updatedColumns?) {
    const params = {
      namespace: getCurrentNamespace(),
      draftId,
    };

    const body: ITableInfo = {
      database: tableInfo.database,
      table: tableInfo.table,
    };

    if (tableInfo.schema) {
      body.schema = tableInfo.schema;
    }

    MyReplicatorApi.assessTable(params, body).subscribe(
      (res) => {
        const sortedColumns = sortBy(res.columns, [
          (column) => {
            switch (column.support) {
              case SUPPORT.no:
                return 0;
              case SUPPORT.partial:
                return 1;
              default:
                return 2;
            }
          },
        ]);

        const key = generateTableKey(tableInfo);
        const existingColumns = updatedColumns ? updatedColumns : columns.get(key);
        const columnRowMap = {};

        if (existingColumns) {
          existingColumns.forEach((column) => {
            const columnName = column.get('name') as string;
            columnRowMap[columnName] = column.get('suppressWarning') || false;
          });
        }

        const checkSuppressedWarning = sortedColumns.map((row) => {
          return {
            ...row,
            suppressWarning: columnRowMap[row.sourceName] || false,
          };
        });

        setAssessmentColumns(checkSuppressedWarning);
        setLoading(false);
      },
      (err) => {
        setError(err);
        setLoading(false);
      }
    );
  }

  React.useEffect(assessTable, []);

  function deleteColumn(column) {
    const key = generateTableKey(tableInfo);

    const existingColumns = columns.get(key);
    let updatedColumns;
    if (existingColumns && existingColumns.size > 0) {
      updatedColumns = existingColumns.filter((value) => {
        return !(
          value.get('name') === column.sourceName && value.get('type') === column.sourceType
        );
      });
    } else {
      const selectedColumns = assessmentColumns
        .filter((col) => {
          return !(col.sourceName === column.sourceName && col.sourceType === column.sourceType);
        })
        .map((col) => {
          return Map({
            name: col.sourceName,
            type: col.sourceType,
          });
        });

      updatedColumns = List(selectedColumns);
    }

    setColumns(columns.set(key, updatedColumns), () => {
      setRerunAssessment(true);
      setLoading(true);
      saveDraft().subscribe(assessTable);
    });
  }

  function ignoreColumn(column) {
    const key = generateTableKey(tableInfo);

    const existingColumns = columns.get(key);
    let updatedColumns = existingColumns;
    if (!existingColumns || existingColumns.size === 0) {
      updatedColumns = List(
        assessmentColumns.map((col) => {
          return Map({
            name: col.sourceName,
            type: col.sourceType,
          });
        })
      );
    }

    updatedColumns = updatedColumns.map((col) => {
      let updatedColumn = col;
      if (col.get('name') === column.sourceName) {
        updatedColumn = updatedColumn.set('suppressWarning', true);
      }
      return updatedColumn;
    }) as IColumnsList;

    setColumns(columns.set(key, updatedColumns), () => {
      setRerunAssessment(true);
      setLoading(true);
      saveDraft().subscribe(assessTable.bind(this, updatedColumns));
    });
  }

  const sourceType = getPluginDisplayName(sourcePluginInfo, sourcePluginWidget);
  const targetType = getPluginDisplayName(targetPluginInfo, targetPluginWidget);

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <div className={classes.backButtonContainer}>
          <span className={classes.backButton} onClick={onClose.bind(null, rerunAssessment)}>
            <span>&laquo;</span>
            <span>Back</span>
          </span>
          <span className={classes.separator}>|</span>
        </div>
        <div>
          <div className={classes.headerPluginTypes}>
            <span>Source: {sourceType}</span>
            <span>&gt;</span>
            <span>Target: {targetType}</span>
          </div>
          <Heading type={HeadingTypes.h4} label={name} />
        </div>
      </div>

      <If condition={error}>
        <div className="text-danger">{JSON.stringify(error, null, 2)}</div>
      </If>

      <If condition={loading}>
        <div className="text-center">
          <br />
          <LoadingSVG />
        </div>
      </If>

      <If condition={!error && !loading}>
        <div className={classes.mappings}>
          <div className="grid-wrapper">
            <div className="grid grid-container grid-compact">
              <div className="grid-row section-heading">
                <div>
                  <div>Source</div>
                  <div>'{tableInfo.table}'</div>
                </div>
                <div>
                  <div>Target</div>
                  <div>'{tableInfo.table}'</div>
                </div>
              </div>

              <div className="grid-header">
                <div className="grid-row">
                  <div className={classes.headerColumnName}>Column name</div>
                  <div className={classes.headerDataTypes}>Data type</div>
                </div>
                <div className="grid-row">
                  <div>Source</div>
                  <div>Target</div>
                  <div>Source</div>
                  <div>&gt;</div>
                  <div>Target</div>
                  <div />
                  <div>Supported</div>
                  <div />
                </div>
              </div>

              <div className="grid-body">
                {assessmentColumns.map((row) => {
                  return (
                    <div className="grid-row" key={row.sourceName}>
                      <div>{row.sourceName}</div>
                      <div>{row.targetName}</div>
                      <div className={classes.source}>{row.sourceType}</div>
                      <div>&gt;</div>
                      <div>{row.targetType}</div>
                      <div />
                      <div>
                        <Supported columnRow={row} />
                      </div>
                      <div>
                        <If condition={!(row.support === SUPPORT.partial && row.suppressWarning)}>
                          <If condition={row.support === SUPPORT.partial}>
                            <span
                              className={classes.columnAction}
                              onClick={ignoreColumn.bind(null, row)}
                            >
                              Ignore
                            </span>
                            <span className={classes.actionsSeparator}>|</span>
                          </If>
                          <If condition={row.support !== SUPPORT.yes}>
                            <span
                              className={classes.columnAction}
                              onClick={deleteColumn.bind(null, row)}
                            >
                              Remove
                            </span>
                          </If>
                        </If>
                        <If condition={row.support === SUPPORT.partial && row.suppressWarning}>
                          <span>Ignored</span>
                        </If>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </If>
    </div>
  );
};

const StyledMappings = withStyles(styles)(MappingsView);
const Mappings = createContextConnect(StyledMappings);
export default Mappings;
