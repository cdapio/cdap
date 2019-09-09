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
import Summary from '../../Summary';
import StepButtons from '../../StepButtons';
import { createReplicator } from 'components/Transfers/utilities';
import If from 'components/If';
import { Redirect } from 'react-router';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (): StyleRules => {
  return {
    root: {
      overflowY: 'auto',
      width: '100%',
    },
    btnContainer: {
      padding: '5px 25px',
      marginBottom: '35px',
    },
  };
};

interface IConfigureSummary extends WithStyles<typeof styles> {
  setStage: (stage) => void;
  getRequestBody: () => any;
  id: string;
}

const ConfigureSummaryView: React.SFC<IConfigureSummary> = ({ classes, getRequestBody, id }) => {
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState(null);
  const [redirect, setRedirect] = React.useState(false);

  function onComplete() {
    setLoading(true);
    setError(null);

    // tslint:disable-next-line:no-console
    console.log('complete!');

    createReplicator(id, getRequestBody()).subscribe(
      () => {
        setRedirect(true);
      },
      (err) => {
        setError(JSON.stringify(err));
        setLoading(false);
      }
    );
  }

  return (
    <div className={classes.root}>
      <Summary />
      <br />
      <If condition={error}>
        <div className="text-danger">{error}</div>
      </If>
      <div className={classes.btnContainer}>
        <StepButtons onComplete={onComplete} loading={loading} />
      </div>

      {redirect ? <Redirect to={`/ns/${getCurrentNamespace()}/transfers/details/${id}`} /> : null}
    </div>
  );
};

const StyledConfigureSummary = withStyles(styles)(ConfigureSummaryView);
const ConfigureSummary = transfersCreateConnect(StyledConfigureSummary);
export default ConfigureSummary;
