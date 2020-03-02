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
import Button from '@material-ui/core/Button';
import { MyReplicatorApi } from 'api/replicator';
import { getCurrentNamespace } from 'services/NamespaceStore';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import capitalize from 'lodash/capitalize';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import Heading, { HeadingTypes } from 'components/Heading';

function convertHexToRGB(color) {
  function parseHex(hex) {
    return parseInt(hex, 16);
  }

  const red = color.substring(1, 3);
  const green = color.substring(3, 5);
  const blue = color.substring(5, 7);

  return `${parseHex(red)}, ${parseHex(green)}, ${parseHex(blue)}`;
}

const styles = (theme): StyleRules => {
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
      gridTemplateColumns: '70% 30%',
      height: '60px',
      backgroundColor: theme.palette.grey[700],
      borderBottom: `1px solid ${theme.palette.grey[400]}`,
      alignContent: 'center',
      '& > div': {
        padding: '0 15px',
      },
    },
    actionButtons: {
      textAlign: 'right',
    },
    mappings: {
      '& .grid-wrapper': {
        '& .grid.grid-container.grid-compact': {
          '& .grid-row': {
            gridTemplateColumns: '120px 2fr 1fr 2fr 1fr',
            '& > div': {
              '&:nth-child(3)': {
                borderRight: `1px solid ${theme.palette.grey[400]}`,
              },
            },
          },

          '& .grid-body .grid-row > div': {
            '&:first-child': {
              borderLeft: `7px solid`,
            },
          },
        },
      },
    },
    supportContainer: {
      '& > span': {
        marginLeft: '5px',
      },
    },
    source: {
      gridColumn: '1 / span 3',
      borderRight: `1px solid ${theme.palette.grey[400]}`,
    },
    target: {
      gridColumn: '4 / span 2',
    },
    green: {
      color: theme.palette.green[50],
    },
    red: {
      color: theme.palette.red[100],
    },
    // For some reason, losing specificity on the border color, that is why there is !important. Will need to
    // investigate how to avoid this.
    greenBox: {
      borderLeftColor: `rgba(${convertHexToRGB(theme.palette.green[50])}, 0.6) !important`,
    },
    redBox: {
      borderLeftColor: `rgba(${convertHexToRGB(theme.palette.red[100])}, 0.3) !important`,
    },
    supportIcon: {
      marginTop: '-2px',
    },
  };
};

interface IMappingsProps extends ICreateContext, WithStyles<typeof styles> {
  tableInfo: {
    database: string;
    table: string;
  };
  onClose: () => void;
}

const MappingsView: React.FC<IMappingsProps> = ({ classes, tableInfo, onClose, draftId }) => {
  const [columns, setColumns] = React.useState([]);
  const [error, setError] = React.useState(null);
  const [loading, setLoading] = React.useState(true);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      draftId,
    };

    const body = {
      database: tableInfo.database,
      table: tableInfo.table,
    };

    MyReplicatorApi.assessTable(params, body).subscribe(
      (res) => {
        setColumns(res.columns);
        setLoading(false);
      },
      (err) => {
        setError(err);
        setLoading(false);
      }
    );
  }, []);

  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <div>
          <Heading type={HeadingTypes.h4} label={tableInfo.table} />
        </div>
        <div className={classes.actionButtons}>
          <Button color="primary" onClick={onClose}>
            Close
          </Button>
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
              <div className="grid-row">
                <div className={classes.source}>
                  <h5>SOURCE</h5>
                </div>
                <div className={classes.target}>
                  <h5>TARGET</h5>
                </div>
              </div>

              <div className="grid-header">
                <div className="grid-row">
                  <div>Supported</div>
                  <div>Name</div>
                  <div>Data type</div>
                  <div>Name</div>
                  <div>Data type</div>
                </div>
              </div>

              <div className="grid-body">
                {columns.map((row, i) => {
                  return (
                    <div className="grid-row" key={row.sourceName}>
                      <div
                        className={classnames(classes.supportContainer, {
                          [classes.greenBox]: row.support === 'YES',
                          [classes.redBox]: row.support !== 'YES',
                        })}
                      >
                        <IconSVG
                          name={row.support === 'PARTIAL' ? 'icon-circle-o' : 'icon-circle'}
                          className={classnames(classes.supportIcon, {
                            [classes.green]: row.support === 'YES',
                            [classes.red]: row.support !== 'YES',
                          })}
                        />
                        <span>{capitalize(row.support)}</span>
                      </div>
                      <div>{row.sourceName}</div>
                      <div>{row.sourceType}</div>
                      <div>{row.targetName}</div>
                      <div>{row.targetType}</div>
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
