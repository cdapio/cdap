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

import PropTypes from 'prop-types';
import * as React from 'react';
import Heading, { HeadingTypes } from 'components/Heading';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import isEmpty from 'lodash/isEmpty';
import { INode, IConnection } from 'components/PipelineContextMenu/PipelineTypes';
import { DataTable } from 'components/PreviewDataView/DataTable';

const stylesForAngular = (theme): StyleRules => ({
  heading: {
    fontSize: '1.5rem',
    textAlign: 'center',
  },
});
const styles = (theme) => {
  return {
    heading: stylesForAngular(theme).heading,
  };
};

interface IGraph {
  stages: INode[];
  connections: IConnection[];
}
interface IPreviewDataProps extends WithStyles<typeof styles> {
  getPreviewId: () => string;
  currentStage: string;
  getStagesAndConnections: () => IGraph;
}

function BasePreviewDataView({
  classes,
  getPreviewId,
  currentStage,
  getStagesAndConnections,
}: IPreviewDataProps) {
  const previewId = getPreviewId();
  const { stages, connections } = getStagesAndConnections();
  return (
    <div>
      <If condition={isEmpty(previewId)}>
        <Heading
          className={classes.heading}
          type={HeadingTypes.h3}
          label={`Preview Data for stage ${currentStage} is not available`}
        />
      </If>
      <If condition={!isEmpty(previewId)}>
        <DataTable
          currentStage={currentStage}
          previewId={previewId}
          stages={stages}
          connections={connections}
        />
      </If>
    </div>
  );
}
const StyledPreviewDataView = withStyles(styles)(BasePreviewDataView);
function PreviewDataView(props) {
  return <StyledPreviewDataView {...props} />;
}
PreviewDataView.propTypes = {
  getPreviewId: PropTypes.func,
  currentStage: PropTypes.string,
  getStagesAndConnections: PropTypes.func,
};
export { PreviewDataView };
