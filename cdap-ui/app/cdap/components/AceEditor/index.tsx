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

import React from 'react';
import 'ace-builds/src-min-noconflict/ace';
import ThemeWrapper from 'components/ThemeWrapper';
import PropTypes from 'prop-types';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

const styles = (theme) => {
  return {
    root: {
      border: `1px solid ${theme.palette.grey['300']}`,
      borderRadius: 4,
      margin: '10px 0 10px 10px',
    },
  };
};
interface IAceEditorProps extends WithStyles<typeof styles> {
  mode?: string;
  value: string;
  onChange: (value: string) => void;
  rows?: number;
  className?: string;
}
class AceEditor extends React.Component<IAceEditorProps> {
  public static LINE_HEIGHT = 20;
  public static defaultProps = {
    mode: 'plain_text',
    value: '',
    rows: 5,
  };
  public aceRef: HTMLElement;
  public componentDidMount() {
    window.ace.config.set('basePath', '/assets/bundle/ace-editor-worker-scripts/');
    const editor = window.ace.edit(this.aceRef);
    editor.getSession().setMode(`ace/mode/${this.props.mode}`);
    editor.getSession().setUseWrapMode(true);
    editor.getSession().on('change', () => {
      if (typeof this.props.onChange === 'function') {
        this.props.onChange(editor.getSession().getValue());
      }
    });
    editor.setShowPrintMargin(false);
  }
  public shouldComponentUpdate() {
    return false;
  }
  public render() {
    const { value, className, classes } = this.props;
    return (
      <div
        className={`${className} ${classes.root}`}
        style={{ height: `${this.props.rows * AceEditor.LINE_HEIGHT}px` }}
        ref={(ref) => (this.aceRef = ref)}
      >
        {value}
      </div>
    );
  }
}
const AceEditorWrapper = withStyles(styles)(AceEditor);
export default function StyledAceEditor(props) {
  return (
    <ThemeWrapper>
      <AceEditorWrapper {...props} />
    </ThemeWrapper>
  );
}

(StyledAceEditor as any).propTypes = {
  mode: PropTypes.string,
  value: PropTypes.string,
  onChange: PropTypes.func,
  rows: PropTypes.number,
};
