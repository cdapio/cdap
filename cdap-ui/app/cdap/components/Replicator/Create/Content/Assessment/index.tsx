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
import Heading, { HeadingTypes } from 'components/Heading';
import { extractErrorMessage } from 'services/helpers';
import Button from '@material-ui/core/Button';
import Refresh from '@material-ui/icons/Refresh';

const contentHeight = 'calc(100% - 27px - 87px - 110px)'; // 100% - heading - link - StepButtons

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '10px 40px',
    },
    title: {
      marginBottom: '5px',
    },
    heading: {
      display: 'grid',
      gridTemplateColumns: '1fr 150px',

      '& > div:last-child': {
        textAlign: 'right',
        paddingTop: '10px',
      },
    },
    subHeading: {
      color: theme.palette.grey[100],
    },
    btnText: {
      marginRight: '5px',
    },
    headerLinks: {
      marginBottom: '20px',
      marginTop: '20px',
    },
    link: {
      fontSize: '16px',
      marginRight: '75px',
      cursor: 'pointer',
      color: theme.palette.grey[100],
      '&:not($active):hover': {
        borderBottom: `2px solid ${theme.palette.grey[300]}`,
      },
    },
    active: {
      fontWeight: 600,
      color: theme.palette.grey[50],
      borderBottom: `5px solid ${theme.palette.grey[300]}`,
    },
    contentContainer: {
      height: contentHeight,
    },
    schemaContentContainer: {
      minHeight: contentHeight,
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
  const [schemaErrorCount, setSchemaErrorCount] = React.useState(0);
  const [view, setView] = React.useState(VIEWS.tables);
  const [assessment, setAssessment] = React.useState({
    tables: [],
    features: [],
    connectivity: [],
  });

  function runAssessment() {
    setLoading(true);
    setError(null);

    const params = {
      namespace: getCurrentNamespace(),
      draftId,
    };

    MyReplicatorApi.assessPipeline(params).subscribe(
      (res) => {
        let schemaError = 0;
        res.tables.forEach((table) => {
          if (table.numColumnsPartiallySupported !== 0 || table.numColumnsNotSupported !== 0) {
            schemaError++;
          }
        });

        setSchemaErrorCount(schemaError);
        setAssessment(res);
        setLoading(false);
      },
      (err) => {
        setError(extractErrorMessage(err));
        setLoading(false);
      }
    );
  }

  React.useEffect(runAssessment, []);

  if (loading) {
    return <LoadingSVGCentered />;
  }

  return (
    <div className={classes.root}>
      <div className={classes.heading}>
        <div>
          <Heading type={HeadingTypes.h4} label="Assessment summary" className={classes.title} />
          <div className={classes.subHeading}>Resolve all issues to continue</div>
        </div>
        <div>
          <Button color="primary" onClick={runAssessment}>
            <span className={classes.btnText}>Refresh</span>
            <Refresh />
          </Button>
        </div>
      </div>

      <If condition={error}>
        <React.Fragment>
          <br />
          <div className="text-danger">
            <Heading type={HeadingTypes.h5} label="Error" />
            <span>{error}</span>
          </div>
        </React.Fragment>
      </If>

      <If condition={!loading && !error}>
        <React.Fragment>
          <div className={classes.headerLinks}>
            <span
              className={classnames(classes.link, { [classes.active]: view === VIEWS.tables })}
              onClick={() => setView(VIEWS.tables)}
            >
              Schema issues ({schemaErrorCount})
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

          <div
            className={classnames({
              [classes.contentContainer]: view !== VIEWS.tables,
              [classes.schemaContentContainer]: view === VIEWS.tables,
            })}
          >
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
