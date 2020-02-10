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
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyReplicatorApi } from 'api/replicator';
import TablesAssessment from 'components/Replicator/Create/Content/Assessment/TablesAssessment';
import If from 'components/If';
import classnames from 'classnames';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import ConnectivityAssessment from 'components/Replicator/Create/Content/Assessment/ConnectivityAssessment';
import FeaturesAssessment from 'components/Replicator/Create/Content/Assessment//FeaturesAssessment';
import StepButtons from 'components/Replicator/Create/Content/StepButtons';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
    },
    headerLinks: {
      marginBottom: '20px',
      marginTop: '30px',
    },
    link: {
      fontSize: '18px',
      marginRight: '75px',
      cursor: 'pointer',
      '&:hover': {
        borderBottom: `3px solid ${theme.palette.grey[300]}`,
      },
    },
    active: {
      fontWeight: 600,
      borderBottom: `3px solid ${theme.palette.grey[300]}`,
    },
    contentContainer: {
      height: 'calc(100% - 27px - 77px - 110px)',
    },
  };
};

enum VIEWS {
  tables = 'tables',
  features = 'features',
  connectivity = 'connectivity',
}

const AssessmentView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  draftId,
}) => {
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(null);
  const [assessment, setAssessment] = React.useState({
    tables: [],
    features: [],
    connectivity: [],
  });

  const [view, setView] = React.useState(VIEWS.tables);

  React.useEffect(() => {
    const params = {
      namespace: getCurrentNamespace(),
      draftId,
    };

    MyReplicatorApi.assessPipeline(params).subscribe(
      (res) => {
        setAssessment(res);
        setLoading(false);
      },
      (err) => {
        setError(err);
        setLoading(false);
      }
    );
  }, []);

  if (loading) {
    return <LoadingSVGCentered />;
  }

  return (
    <div className={classes.root}>
      <h3>Assessment summary</h3>

      <If condition={error}>
        <div className="text-danger">{JSON.stringify(error, null, 2)}</div>
      </If>

      <If condition={!loading && !error}>
        <React.Fragment>
          <div className={classes.headerLinks}>
            <span
              className={classnames(classes.link, { [classes.active]: view === VIEWS.tables })}
              onClick={() => setView(VIEWS.tables)}
            >
              Schema
            </span>
            <span
              className={classnames(classes.link, { [classes.active]: view === VIEWS.features })}
              onClick={() => setView(VIEWS.features)}
            >
              Missing features ({assessment.features.length})
            </span>
            <span
              className={classnames(classes.link, {
                [classes.active]: view === VIEWS.connectivity,
              })}
              onClick={() => setView(VIEWS.connectivity)}
            >
              Connectivity issues ({assessment.connectivity.length})
            </span>
          </div>

          <div className={classes.contentContainer}>
            <If condition={view === VIEWS.tables}>
              <TablesAssessment tables={assessment.tables} />
            </If>

            <If condition={view === VIEWS.features}>
              <FeaturesAssessment features={assessment.features} />
            </If>

            <If condition={view === VIEWS.connectivity}>
              <ConnectivityAssessment connectivity={assessment.connectivity} />
            </If>
          </div>
        </React.Fragment>
      </If>

      <StepButtons />
    </div>
  );
};

const StyledAssessmentView = withStyles(styles)(AssessmentView);
const Assessment = createContextConnect(StyledAssessmentView);
export default Assessment;
