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
import { STEPS } from 'components/PluginCreator/Create/steps';
import { createContextConnect } from 'components/PluginCreator/Create';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';

const styles = (): StyleRules => {
  return {
    comp: {
      borderRight: '1px solid #e0e0e0',
      width: '60%',
      color: '#e0e0e0',
    },
  };
};

interface IContentProps {
  activeStep: number;
}

const ContentView: React.FC<IContentProps & WithStyles<typeof styles>> = ({
  classes,
  activeStep,
}) => {
  if (!STEPS[activeStep] || !STEPS[activeStep].component) {
    return null;
  }

  const Comp = STEPS[activeStep].component;
  return (
    <div>
      <Comp className={classes.comp} />
    </div>
  );
};

const StyledContentView = withStyles(styles)(ContentView);
const Content = createContextConnect(StyledContentView);
export default Content;
