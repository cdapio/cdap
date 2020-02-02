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
import StepButtons from 'components/Replicator/Create/Content/StepButtons';
import { constructReplicatorSpec } from 'components/Replicator/utilities';
import If from 'components/If';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import { Redirect } from 'react-router-dom';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
    },
    summary: {
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      '& > pre': {
        wordBreak: 'break-word',
        whiteSpace: 'pre-wrap',
        padding: '15px',
      },
    },
    error: {
      color: theme.palette.red[100],
    },
  };
};

const SummaryView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  sourcePluginInfo,
  targetPluginInfo,
  sourceConfig,
  targetConfig,
  name,
  description,
  draftId,
}) => {
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState(null);
  const [redirect, setRedirect] = React.useState(false);
  function constructJson() {
    return constructReplicatorSpec(
      name,
      description,
      sourcePluginInfo,
      targetPluginInfo,
      sourceConfig,
      targetConfig
    );
  }

  function publish() {
    setLoading(true);
    const spec = constructJson();

    const params = {
      namespace: getCurrentNamespace(),
      appName: spec.name,
    };

    MyReplicatorApi.publish(params, spec).subscribe(
      () => {
        MyReplicatorApi.deleteDraft({
          namespace: getCurrentNamespace(),
          draftId,
        }).subscribe(null, null, () => {
          setRedirect(true);
        });
      },
      (err) => {
        setError(err);
        setLoading(false);
      }
    );
  }

  if (redirect) {
    return <Redirect to={`/ns/${getCurrentNamespace()}/replicator`} />;
  }

  return (
    <div className={classes.root}>
      <div className={classes.summary}>
        <pre>{JSON.stringify(constructJson(), null, 2)}</pre>
      </div>

      <If condition={error}>
        <div className={classes.error}>{JSON.stringify(error, null, 2)}</div>
      </If>

      <StepButtons onComplete={publish} completeLoading={loading} />
    </div>
  );
};

const StyledSummary = withStyles(styles)(SummaryView);
const Summary = createContextConnect(StyledSummary);
export default Summary;
