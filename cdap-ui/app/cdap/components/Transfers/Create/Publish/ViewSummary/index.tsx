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
import { transfersCreateConnect, Stages } from 'components/Transfers/Create/context';
import Summary from '../../Summary';
import Button from '@material-ui/core/Button';
import If from 'components/If';
import LoadingSVG from 'components/LoadingSVG';
import { createTransfer } from 'components/Transfers/utilities';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyDeltaApi } from 'api/delta';
import { Redirect } from 'react-router-dom';

const styles = (): StyleRules => {
  return {};
};

interface IViewSummary extends WithStyles<typeof styles> {
  id: string;
  description: string;
  source: any;
  target: any;
  getRequestBody: (activeStep) => any;
}

const ViewSummaryView: React.SFC<IViewSummary> = ({
  id,
  description,
  source,
  target,
  getRequestBody,
}) => {
  const [loading, setLoading] = React.useState(false);
  const [redirect, setRedirect] = React.useState(false);
  const [error, setError] = React.useState();
  function onCreate() {
    setLoading(true);

    const generatedName = `CDC-${id}`;

    createTransfer(generatedName, description, source, target).subscribe(
      () => {
        const defaultRequestBody = getRequestBody(0);

        const requestBody = {
          ...defaultRequestBody,
          properties: {
            ...defaultRequestBody.properties,
            stage: Stages.PUBLISHED,
            pipelineName: generatedName,
          },
        };

        const params = {
          context: getCurrentNamespace(),
          id,
        };

        MyDeltaApi.update(params, requestBody).subscribe(
          () => {
            setRedirect(true);
          },
          (err) => {
            setLoading(false);
            setError(`Update Instance Store Failed\n${JSON.stringify(err, null, 2)}`);
            // tslint:disable-next-line:no-console
            console.log('error', err);
          }
        );
      },
      (err) => {
        setLoading(false);
        setError(`Failed to create app\n${JSON.stringify(err, null, 2)}`);
      }
    );
  }

  if (redirect) {
    return <Redirect to={`/ns/${getCurrentNamespace()}/transfers/details/${id}`} />;
  }

  return (
    <div>
      <Summary />
      <br />

      <If condition={error && error.length > 0}>
        <div className="text-danger">{error}</div>
      </If>

      <Button variant="contained" color="primary" onClick={onCreate} disabled={loading}>
        <If condition={loading}>
          <LoadingSVG />
        </If>
        Create
      </Button>
    </div>
  );
};

const StyledViewSummary = withStyles(styles)(ViewSummaryView);
const ViewSummary = transfersCreateConnect(StyledViewSummary);
export default ViewSummary;
