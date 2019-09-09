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
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import { objectQuery } from 'services/helpers';
import { Theme } from 'services/ThemeHelper';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '15px 25px',
      '& > hr': {
        borderWidth: '3px',
      },
    },
    headingContainer: {
      marginTop: '15px',
      marginBottom: '15px',
    },
    heading: {
      display: 'inline-block',
    },
    edit: {
      marginLeft: '20px',
      color: theme.palette.blue[100],
      cursor: 'pointer',
      '&:hover': {
        textDecoration: 'underline',
      },
    },
    summaryContent: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gridColumnGap: '50px',
    },
    table: {
      marginBottom: '25px',
      '& td': {
        width: '50%',
        whiteSpace: 'pre',
      },
      '& th': {
        borderTop: '0',
        color: theme.palette.grey[200],
      },
    },
  };
};

interface ISummaryProps extends WithStyles<typeof styles> {
  name: string;
  description: string;
  source: any;
  sourceConfig: any;
  target: any;
  targetConfig: any;
  disableEdit: boolean;
  setActiveStep?: (step) => void;
}

const SummaryView: React.SFC<ISummaryProps> = ({
  name,
  description,
  source,
  sourceConfig,
  target,
  targetConfig,
  // tslint:disable-next-line:no-empty
  setActiveStep = (step) => {},
  disableEdit,
  classes,
}) => {
  const sourceGroups = objectQuery(sourceConfig, 'configuration-groups');
  const targetGroups = objectQuery(targetConfig, 'configuration-groups');

  const tables = (objectQuery(source, 'plugin', 'properties', 'tableWhiteList') || '')
    .split(',')
    .map((fullTable) => {
      return fullTable.split('.')[1];
    });

  return (
    <div className={classes.root}>
      <div>Review information and create {Theme.featureNames.transfers.toLowerCase()}</div>

      <div className={classes.headingContainer}>
        <div>
          <h3 className={classes.heading}>
            <span>{name}</span>
          </h3>
          <If condition={!disableEdit}>
            <span onClick={setActiveStep.bind(null, 0)} className={classes.edit}>
              Edit
            </span>
          </If>
        </div>
        <div>{description}</div>
      </div>

      <hr />

      <div className={classes.summaryContent}>
        <div className="source">
          <span>SOURCE</span>
          <div>
            <h4 className={classes.heading}>MySQL Database</h4>
            <If condition={!disableEdit}>
              <span onClick={setActiveStep.bind(null, 2)} className={classes.edit}>
                Edit
              </span>
            </If>
          </div>

          <div>
            {sourceGroups.map((group, i) => {
              return (
                <div key={i}>
                  <table className={`table ${classes.table}`}>
                    <thead>
                      <tr>
                        <th>{group.label}</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {group.properties.map((property) => {
                        if (['hidden', 'password'].indexOf(property['widget-type']) !== -1) {
                          return null;
                        }
                        const propertyValue =
                          objectQuery(source, 'plugin', 'properties', property.name) || '--';
                        return (
                          <tr key={property.name}>
                            <td>
                              <strong>{property.label}</strong>
                            </td>
                            <td>{propertyValue}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              );
            })}
          </div>
        </div>

        <div className="target">
          <span>TARGET</span>
          <div>
            <h4 className={classes.heading}>Google BigQuery</h4>
            <If condition={!disableEdit}>
              <span onClick={setActiveStep.bind(null, 5)} className={classes.edit}>
                Edit
              </span>
            </If>
          </div>
          <div>
            {targetGroups.map((group, i) => {
              return (
                <div key={i}>
                  <table className={`table ${classes.table}`}>
                    <thead>
                      <tr>
                        <th>{group.label}</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {group.properties.map((property) => {
                        if (property.name === 'serviceAccountKey') {
                          return null;
                        }

                        const propertyValue =
                          objectQuery(target, 'plugin', 'properties', property.name) || '--';

                        return (
                          <tr key={property.name}>
                            <td>
                              <strong>{property.label}</strong>
                            </td>
                            <td>{propertyValue}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <hr />

      <div>
        <div>{tables.length} tables to be duplicated</div>
        <br />
        <table className="table">
          <thead>
            <tr>
              <th>Table name</th>
            </tr>
          </thead>

          <tbody>
            {tables.map((table) => {
              return (
                <tr key={table}>
                  <td>{table}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export const StyledSummary = withStyles(styles)(SummaryView);
const Summary = transfersCreateConnect(StyledSummary);
export default Summary;
