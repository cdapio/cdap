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
import { ActionConfig } from 'components/Home/HomeActions/ActionConfig';
import ActionCard from 'components/Home/HomeActions/ActionCard';
import Welcome from 'components/Home/Welcome';
import ExperimentalFeature from 'components/Lab/ExperimentalFeature';

const styles = (): StyleRules => {
  return {
    root: {
      height: '100%',
    },
    scrollContainer: {
      height: '100%',
      overflowY: 'auto',
    },
    cardsContainer: {
      paddingTop: '25px',
      paddingBottom: '25px',
      display: 'flex',
      flexWrap: 'wrap',
      alignItems: 'stretch',
      width: '810px',
      margin: '0 auto',
    },
  };
};

const HomeActionsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div className={classes.root}>
      <div className={classes.scrollContainer}>
        <div className={classes.cardsContainer}>
          {ActionConfig.map((action) => {
            if (action.featureFlag === false) {
              return null;
            }

            return action.experiment ? (
              <ExperimentalFeature key={action.title} name={action.experiment}>
                <ActionCard key={action.title} config={action} />
              </ExperimentalFeature>
            ) : (
              <ActionCard key={action.title} config={action} />
            );
          })}
        </div>
      </div>
      <Welcome />
    </div>
  );
};

const HomeActions = withStyles(styles)(HomeActionsView);
export default HomeActions;
