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
import StepButtons from 'components/Transfers/Create/StepButtons';
import { createTransfer } from 'components/Transfers/utilities';
import { Redirect } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { objectQuery } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '15px 50px',
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
      gridTemplateColumns: '50% 50%',
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
  setActiveStep: (step) => void;
}

const SummaryView: React.SFC<ISummaryProps> = ({
  name,
  description,
  source,
  sourceConfig,
  target,
  targetConfig,
  setActiveStep,
  classes,
}) => {
  const [redirect, setRedirect] = React.useState(false);
  const [loading, setLoading] = React.useState(false);

  function onComplete() {
    setLoading(true);
    createTransfer(name, description, source, target).subscribe(
      () => {
        setRedirect(true);
      },
      (err) => {
        // tslint:disable-next-line:no-console
        console.log('error', err);
      },
      () => {
        setLoading(false);
      }
    );
  }

  if (redirect) {
    return <Redirect to={`/ns/${getCurrentNamespace()}/transfers`} />;
  }

  const sourceGroups = objectQuery(sourceConfig, 'configuration-groups');
  const targetGroups = objectQuery(targetConfig, 'configuration-groups');

  return (
    <div className={classes.root}>
      <div>Review information and create transfer</div>

      <div className={classes.headingContainer}>
        <div>
          <h3 className={classes.heading}>
            <span>{name}</span>
          </h3>
          <span onClick={setActiveStep.bind(null, 0)} className={classes.edit}>
            Edit
          </span>
        </div>
        <div>{description} description</div>
      </div>

      <div className={classes.summaryContent}>
        <div className="source">
          <div>
            <h4 className={classes.heading}>Oracle Database</h4>
            <span onClick={setActiveStep.bind(null, 1)} className={classes.edit}>
              Edit
            </span>
          </div>

          <div>
            {sourceGroups.map((group, i) => {
              return (
                <div key={i}>
                  <table className={`table ${classes.table}`}>
                    <thead>
                      <th>{group.label}</th>
                      <th />
                    </thead>
                    <tbody>
                      {group.properties.map((property) => {
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
          <div>
            <h4 className={classes.heading}>Google BigQuery</h4>
            <span onClick={setActiveStep.bind(null, 2)} className={classes.edit}>
              Edit
            </span>
          </div>
          <div>
            {targetGroups.map((group, i) => {
              return (
                <div key={i}>
                  <table className={`table ${classes.table}`}>
                    <thead>
                      <th>{group.label}</th>
                      <th />
                    </thead>
                    <tbody>
                      {group.properties.map((property) => {
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

      <StepButtons onComplete={onComplete} loading={loading} />
    </div>
  );
};

const StyledSummary = withStyles(styles)(SummaryView);
const Summary = transfersCreateConnect(StyledSummary);
export default Summary;
