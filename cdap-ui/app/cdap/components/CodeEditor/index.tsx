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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Button from '@material-ui/core/Button';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'block',
      position: 'relative',
      border: `1px solid ${theme.palette.grey[400]}`,
    },
    button: {
      position: 'absolute',
      right: 0,
      top: 0,
      zIndex: 1000,
      margin: 0,
    },
  };
};

export interface IBaseCodeEditorProps {
  mode?: string;
  value: string;
  onChange: (value: string) => void;
  rows?: number;
  tabSize?: number;
  className?: string;
  disabled?: boolean;
  activeLineMarker?: boolean;
  showPrettyPrintButton?: boolean;
  prettyPrintFunction?: (value: string) => string;
  classes: Record<string, string>;
}

interface ICodeEditorProps extends IBaseCodeEditorProps, WithStyles<typeof styles> {}

class CodeEditorView extends React.Component<ICodeEditorProps> {
  public static LINE_HEIGHT = 20;
  public static defaultProps = {
    mode: 'plain_text',
    value: '',
    rows: 5,
    disabled: false,
    tabSize: 2,
    activeLineMarker: true,
    showPrettyPrintButton: false,
  };
  public aceRef: HTMLElement;
  private editor;

  public componentWillReceiveProps(nextProps) {
    const currentValue = this.editor.getSession().getValue();
    if (nextProps.value === currentValue) {
      return;
    }

    this.editor.getSession().setValue(nextProps.value);
  }

  public componentDidMount() {
    window.ace.config.set('basePath', '/assets/bundle/ace-editor-worker-scripts/');
    this.editor = window.ace.edit(this.aceRef);
    this.editor.getSession().setMode(`ace/mode/${this.props.mode}`);
    this.editor.getSession().setUseWrapMode(true);
    this.editor.getSession().setOptions({ tabSize: this.props.tabSize });
    this.editor.setHighlightActiveLine(this.props.activeLineMarker);
    if (this.props.disabled) {
      this.editor.setReadOnly(true);
    }
    this.editor.getSession().on('change', () => {
      if (typeof this.props.onChange === 'function') {
        this.props.onChange(this.editor.getSession().getValue());
      }
    });
    this.editor.setShowPrintMargin(false);
  }
  public shouldComponentUpdate() {
    return false;
  }
  public render() {
    const { value, className, classes } = this.props;
    return (
      <div className={classes.root}>
        <div
          className={`${className}`}
          style={{ height: `${this.props.rows * CodeEditorView.LINE_HEIGHT}px` }}
          ref={(ref) => (this.aceRef = ref)}
        >
          {value}
        </div>
        <If condition={this.props.showPrettyPrintButton}>
          <Button
            className={classes.button}
            variant="outlined"
            onClick={() => {
              let code = this.editor.getSession().getValue();
              if (typeof this.props.prettyPrintFunction === 'function') {
                code = this.props.prettyPrintFunction(code);
              }
              this.props.onChange(code);
            }}
          >
            Tidy
          </Button>
        </If>
      </div>
    );
  }
}
const StyledCodeEditor = withStyles(styles)(CodeEditorView);
export default function CodeEditor(props) {
  return (
    <ThemeWrapper>
      <StyledCodeEditor {...props} />
    </ThemeWrapper>
  );
}

(CodeEditor as any).propTypes = {
  mode: PropTypes.string,
  value: PropTypes.string,
  onChange: PropTypes.func,
  rows: PropTypes.number,
  disabled: PropTypes.bool,
  tabSize: PropTypes.number,
  activeLineMarker: PropTypes.bool,
  showPrettyPrintButton: PropTypes.bool,
};
