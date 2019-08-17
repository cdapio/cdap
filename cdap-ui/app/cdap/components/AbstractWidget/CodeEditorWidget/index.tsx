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
import { IWidgetProps } from 'components/AbstractWidget';
import CodeEditor from 'components/CodeEditor';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
import PropTypes from 'prop-types';
import ThemeWrapper from 'components/ThemeWrapper';

const styles = (): StyleRules => {
  return {
    root: {
      paddingTop: '7px',
    },
    editorRoot: {
      border: 0,
    },
  };
};

interface ICodeEditorProps extends IWidgetProps<null>, WithStyles<typeof styles> {
  mode: string;
  rows: number;
}

const CodeEditorWidgetView: React.FC<ICodeEditorProps> = ({
  value,
  onChange,
  disabled,
  mode,
  rows,
  classes,
}) => {
  return (
    <div className={classes.root}>
      <CodeEditor
        mode={mode}
        rows={rows}
        value={value}
        onChange={onChange}
        disabled={disabled}
        classes={{ root: classes.editorRoot }}
      />
    </div>
  );
};

const StyledCodeEditorWidget = withStyles(styles)(CodeEditorWidgetView);

function CodeEditorWidget(props) {
  return (
    <ThemeWrapper>
      <StyledCodeEditorWidget {...props} />
    </ThemeWrapper>
  );
}

(CodeEditorWidget as any).propTypes = {
  ...WIDGET_PROPTYPES,
  mode: PropTypes.string,
  rows: PropTypes.number,
};

export default CodeEditorWidget;
