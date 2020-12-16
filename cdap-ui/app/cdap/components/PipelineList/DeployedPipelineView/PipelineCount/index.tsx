/*
 * Copyright © 2018 Cask Data, Inc.
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
import T from 'i18n-react';
import { connect } from 'react-redux';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';

const styles = () => {
  return {
    root: {
      display: 'inline-block',
      marginLeft: '10px',
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  pipelines: IPipeline[];
  pipelinesLoading: boolean;
  filteredPipelines: IPipeline[];
}

const PREFIX = 'features.PipelineList';

const PipelineCountView: React.SFC<IProps> = ({
  pipelines = [],
  pipelinesLoading,
  filteredPipelines,
  classes,
}) => {
  if (pipelinesLoading) {
    return null;
  }
  let count = `Showing ${filteredPipelines.length} of ${pipelines.length}`;
  if (!pipelines.length) {
    count = '0';
  }
  return (
    <div className={classes.root}>
      {T.translate(`${PREFIX}.DeployedPipelineView.pipelineCount`, {
        context: count,
      })}
    </div>
  );
};

const mapStateToProps = (state) => ({
  pipelines: state.deployed.pipelines,
  filteredPipelines: state.deployed.filteredPipelines,
});

const PipelineCount = PipelineCountView;

export default connect(mapStateToProps)(withStyles(styles)(PipelineCount));
