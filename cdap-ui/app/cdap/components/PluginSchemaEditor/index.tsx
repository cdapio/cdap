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
import Select from 'components/AbstractWidget/FormInputs/Select';
import { SchemaEditor } from 'components/AbstractWidget/SchemaEditor';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import ee from 'event-emitter';
import { getDefaultEmptyAvroSchema } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import PropTypes from 'prop-types';
import LoadingSVG from 'components/LoadingSVG';
import If from 'components/If';
import TextboxOnValium from 'components/TextboxOnValium';
import { RefreshableSchemaEditor } from 'components/PluginSchemaEditor/RefreshableSchemaEditor';
import ConfigurableTab from 'components/ConfigurableTab';
import classnames from 'classnames';
import { objectQuery, isNilOrEmptyString } from 'services/helpers';
import { FieldsListBase } from 'components/AbstractWidget/SchemaEditor/FieldsList';
import Alert from 'components/Alert';
import { isObject } from 'vega-lite/build/src/util';
import { isMacro } from 'services/helpers';
import DownloadFile from 'services/download-file';

const styles = (theme): StyleRules => {
  return {
    container: {
      display: 'block',
      height: '100%',
    },
    fieldset: {
      '&[disabled] *': {
        color: `${theme.palette.grey[200]} !important`,
        cursor: 'not-allowed !important',
      },
    },
    tabContent: {
      width: '100%',
    },
    header: {
      display: 'grid',
      gridTemplateColumns: 'auto 100px',
      alignItems: 'center',
      margin: '-15px -10px 5px -10px',
      padding: '0 10px 5px;',
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
    },
    disabledHeader: {
      margin: '0 -10px 5px -10px',
      padding: '0 10px 10px',
    },
    title: {
      fontWeight: 500,
    },
    actionsDropdown: {
      border: `1px solid ${theme.palette.grey[400]}`,
      borderRadius: '4px',
    },
    loadingContainer: {
      textAlign: 'center',
    },
    macroTextBox: {
      width: '100%',
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      padding: '5px',
    },
  };
};

enum SchemaActionsEnum {
  IMPORT = 'import',
  EXPORT = 'export',
  CLEAR = 'clear',
  MACRO = 'macro',
  EDITOR = 'editor',
  PROPAGATE = 'propagate',
}
enum IPluginSchemaEditorModes {
  Macro = 'macro',
  Editor = 'editor',
}
interface IActionsOptionsObj {
  disabled?: boolean;
  tooltip?: string;
  label: string;
  value: SchemaActionsEnum;
  onClick?: () => void;
}
interface IPluginSchema {
  name: string;
  schema: string;
}
type IActionsDropdownTooltip = Record<SchemaActionsEnum, IActionsOptionsObj>;
interface IPluginSchemaEditorState {
  error: string;
  schemas: IPluginSchema[];
  mode: IPluginSchemaEditorModes;
  loading: boolean;
  schemaRowCount: number;
}

interface IPluginSchemaEditorProps extends WithStyles<typeof styles> {
  disabled?: boolean;
  actionsDropdownMap: IActionsDropdownTooltip;
  schemas: IPluginSchema[];
  onSchemaChange: (schemas: IPluginSchema[]) => void;
  schemaTitle?: string;
  isSchemaMacro?: boolean;
}

class PluginSchemaEditorBase extends React.PureComponent<
  IPluginSchemaEditorProps,
  IPluginSchemaEditorState
> {
  private actions: IActionsOptionsObj[] = Object.values(this.props.actionsDropdownMap).map(
    (value) => value
  );
  private containerRef;

  private getMode = () => {
    if (this.props.disabled) {
      return isMacro(objectQuery(this.props.schemas, 0, 'schema') || '')
        ? IPluginSchemaEditorModes.Macro
        : IPluginSchemaEditorModes.Editor;
    }
    return this.props.isSchemaMacro
      ? IPluginSchemaEditorModes.Macro
      : IPluginSchemaEditorModes.Editor;
  };

  public state = {
    error: null,
    schemas: this.props.schemas,
    loading: false,
    mode: this.getMode(),
    schemaRowCount: null,
  };

  private ee = ee(ee);

  constructor(props) {
    super(props);
    if (!this.props.disabled) {
      this.ee.on('schema.import', this.onSchemaImport);
      this.ee.on('dataset.selected', this.onSchemaImport);
    }
    const doesActionDropdownHasExport = Object.keys(this.props.actionsDropdownMap).find(
      (key) => key.toLowerCase() === 'export'
    );
    if (doesActionDropdownHasExport) {
      // Schema can be exported on detailed view even if it is disabled.
      this.ee.on('schema.export', this.onSchemaExport);
    }
    window.addEventListener('resize', this.calculateSchemaRowCount);
  }

  public componentWillReceiveProps(nextProps: IPluginSchemaEditorProps) {
    if (!Object.keys(nextProps.actionsDropdownMap).length) {
      this.actions = [];
      return;
    }
    this.actions = Object.values(nextProps.actionsDropdownMap).map((value) => value);
    return;
  }

  public shouldComponentUpdate(nextProps: IPluginSchemaEditorProps, nextState) {
    const { disabled, isSchemaMacro, actionsDropdownMap, schemas } = nextProps;
    const { schemaRowCount, loading, mode, error } = nextState;
    const newActions = Object.values(actionsDropdownMap)
      .filter((value) => typeof value === 'string')
      .join('--');
    const existingActions = Object.values(this.props.actionsDropdownMap)
      .filter((value) => typeof value === 'string')
      .join('--');
    const existingSchemas = this.props.schemas.map((s) => s.schema).join('__');
    const newSchemas = schemas.map((s) => s.schema).join('__');

    const didPropsChange =
      disabled !== this.props.disabled ||
      isSchemaMacro !== this.props.isSchemaMacro ||
      newActions !== existingActions ||
      newSchemas !== existingSchemas;

    const didStateChange =
      schemaRowCount !== this.state.schemaRowCount ||
      loading !== this.state.loading ||
      mode !== this.state.mode ||
      error !== this.state.error;
    return didStateChange || didPropsChange;
  }

  public componentDidMount() {
    this.calculateSchemaRowCount();
  }

  public componentWillUnmount() {
    this.ee.off('schema.import', this.onSchemaImport);
    this.ee.off('schema.export', this.onSchemaExport);
    this.ee.on('dataset.selected', this.onSchemaImport);
    window.removeEventListener('resize', this.calculateSchemaRowCount);
  }

  public calculateSchemaRowCount = () => {
    if (this.containerRef) {
      const { height } = this.containerRef.getBoundingClientRect();
      let schemaRowCount = Math.floor(height / FieldsListBase.heightOfRow) - 1;
      // For sinks we can't determine the height. So set it to by default 20 (20 * 34 = 680px in height)
      if (schemaRowCount < 5) {
        schemaRowCount = 20;
      }
      this.setState({ schemaRowCount });
    }
  };

  public onSchemaExport = () => {
    const schemasToExport = this.props.schemas.map((schema) => {
      try {
        return {
          name: schema.name,
          schema: JSON.parse(schema.schema),
        };
      } catch (e) {
        return schema;
      }
    });
    // // CDAP-17106 - Need to use generic DownloadFile function here from download-file
    const blob = new Blob([JSON.stringify(schemasToExport, null, 4)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const exportFileName = 'schema';
    const a = document.createElement('a');
    a.href = url;
    a.download = `${exportFileName}.json`;

    const clickHandler = (event) => {
      event.stopPropagation();
    };
    a.addEventListener('click', clickHandler, false);
    a.click();
  };

  private onSchemaImport = (schemas) => {
    if (this.state.mode === IPluginSchemaEditorModes.Macro) {
      return;
    }
    let importedSchemas = schemas;
    if (typeof schemas === 'string') {
      try {
        importedSchemas = JSON.parse(schemas);
      } catch (e) {
        this.setState({
          error: e.message,
        });
        return;
      }
    }
    let hasError = false;
    if (!Array.isArray(importedSchemas)) {
      // This will be the case when we hit 'Apply' button from wrangler.
      // We don't maintain consistency in importing schemas from multiple
      // sources. This will take time to fix as we right now throw schemas
      // in multiple formats across all places.
      if (isObject(importedSchemas) && !importedSchemas.hasOwnProperty('schema')) {
        importedSchemas = { name: 'etlSchemaBody', schema: importedSchemas };
      }
      importedSchemas = [importedSchemas];
    }
    const newSchemas = importedSchemas.map((schema) => {
      if (typeof schema !== 'object' && schema.hasOwnProperty('schema')) {
        return { ...getDefaultEmptyAvroSchema() };
      }
      const s = { ...schema };
      try {
        if (typeof schema.schema === 'string') {
          s.schema = JSON.parse(schema.schema);
        }
      } catch (e) {
        this.setState({
          error: e.message,
        });
        hasError = true;
        return;
      }
      if (!s.schema || s.schema.type !== 'record' || !Array.isArray(s.schema.fields)) {
        this.setState({
          error: 'Imported schema is not a valid Avro schema',
        });
        hasError = true;
        return;
      }
      if (s.schema.name) {
        s.schema.name = s.schema.name.replace('.', '.type');
      }
      return s;
    });
    if (hasError) {
      return;
    }
    this.setState({ schemas: newSchemas, loading: true }, () => {
      setTimeout(() => {
        this.setState({
          loading: false,
        });
        const schemasForPlugin = newSchemas.map((s) => {
          if (typeof s.schema !== 'string') {
            s.schema = JSON.stringify(s.schema);
          }
          return s;
        });
        this.props.onSchemaChange(schemasForPlugin);
      }, 1000);
    });
  };

  private onActionsHandler = (value) => {
    const specificAction = this.actions.find((action) => action.value === value);
    if (!specificAction) {
      return;
    }
    specificAction.onClick();
    if (value === SchemaActionsEnum.CLEAR) {
      specificAction.onClick();
      this.setState(
        {
          loading: true,
          schemas: [{ name: 'etlSchemaBody', schema: '' }],
        },
        () => {
          setTimeout(() => {
            this.setState({
              loading: false,
            });
          }, 1000);
        }
      );
    } else if (value === SchemaActionsEnum.MACRO) {
      const newState = {
        mode:
          this.state.mode === IPluginSchemaEditorModes.Editor
            ? IPluginSchemaEditorModes.Macro
            : IPluginSchemaEditorModes.Editor,
        schemas:
          this.state.mode === IPluginSchemaEditorModes.Editor
            ? [{ name: 'etlSchemaBody', schema: '${}' }]
            : [{ name: 'etlSchemaBody', schema: '' }],
      };
      this.setState(newState);
    }
  };

  private santizeSchemasForEditor = (schemas = this.state.schemas) => {
    return (schemas || []).map((s) => {
      const newSchema = {
        name: s.name,
        schema: s.schema,
      };
      if (typeof s.schema === 'string') {
        try {
          newSchema.schema = JSON.parse(s.schema);
        } catch (e) {
          return { ...getDefaultEmptyAvroSchema() };
        }
      }
      return newSchema;
    });
  };

  public renderSchemaIntabs = () => {
    const tabs = this.santizeSchemasForEditor().map((s, i) => {
      return {
        id: i,
        name: s.name,
        content: (
          <fieldset disabled={this.props.disabled} className={this.props.classes.fieldset} key={i}>
            <RefreshableSchemaEditor
              visibleRows={this.state.schemaRowCount}
              schema={s}
              disabled={this.props.disabled}
              onChange={({ avroSchema }) => {
                const newSchemas = [...this.props.schemas];
                newSchemas[i] = avroSchema;
                newSchemas[i].schema = JSON.stringify(newSchemas[i].schema);
                this.props.onSchemaChange(newSchemas);
              }}
            />
          </fieldset>
        ),
        contentClassName: this.props.classes.tabContent,
        paneClassName: this.props.classes.tabContent,
      };
    });
    const tabProps = {
      activeTab: 0,
      tabConfig: {
        tabs,
        layout: 'horizontal',
        defaultTab: 0,
      },
    };
    return <ConfigurableTab activeTab={tabProps.activeTab} tabConfig={tabProps.tabConfig} />;
  };

  public renderSchemaEditors = () => {
    if (
      this.state.loading ||
      this.state.mode === IPluginSchemaEditorModes.Macro ||
      !this.state.schemaRowCount
    ) {
      return null;
    }
    if (this.state.schemas.length > 1) {
      return this.renderSchemaIntabs();
    }
    if (!this.state.schemas.length && this.props.disabled) {
      return `No ${this.props.schemaTitle} available`;
    }
    return this.santizeSchemasForEditor().map((schema, i) => (
      <fieldset disabled={this.props.disabled} className={this.props.classes.fieldset}>
        <SchemaEditor
          key={i}
          visibleRows={this.state.schemaRowCount}
          schema={schema}
          disabled={this.props.disabled}
          onChange={({ avroSchema }) => {
            const newSchemas = [...this.props.schemas];
            newSchemas[i] = avroSchema;
            newSchemas[i].schema = JSON.stringify(newSchemas[i].schema);
            this.props.onSchemaChange(newSchemas);
          }}
        />
      </fieldset>
    ));
  };

  public renderMacroEditor = () => {
    if (
      this.state.loading ||
      this.state.mode === IPluginSchemaEditorModes.Editor ||
      !this.state.schemaRowCount
    ) {
      return null;
    }
    const macro = this.state.schemas[0].schema;
    if (this.props.disabled) {
      return macro;
    }
    return (
      <TextboxOnValium
        disabled={this.props.disabled}
        value={macro}
        className={this.props.classes.macroTextBox}
        onKeyUp={() => ({})}
        onChange={(value) => {
          const newSchemas = [{ name: 'etlSchemaBody', schema: value }];
          this.setState({
            schemas: newSchemas,
          });
          this.props.onSchemaChange(newSchemas);
        }}
      />
    );
  };

  public renderHeader = () => {
    const { classes, schemaTitle, disabled } = this.props;
    return (
      <div
        className={classnames(classes.header, {
          [classes.disabledHeader]: disabled,
        })}
      >
        <div className={classes.title}>{schemaTitle || 'Schema'}</div>
        <If condition={this.actions.length > 0}>
          <Select
            classes={{ root: classes.actionsDropdown }}
            value={''}
            placeholder="Actions"
            onChange={this.onActionsHandler}
            widgetProps={{ options: this.actions, dense: true }}
          />
        </If>
      </div>
    );
  };

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.container} ref={(ref) => (this.containerRef = ref)}>
        {this.renderHeader()}
        <If condition={this.state.loading}>
          <div className={classes.loadingContainer}>
            <LoadingSVG />
          </div>
        </If>
        <Alert
          showAlert={!isNilOrEmptyString(this.state.error)}
          message={this.state.error}
          type="error"
          onClose={() => {
            this.setState({ error: null });
          }}
        />
        {this.renderSchemaEditors()}
        {this.renderMacroEditor()}
      </div>
    );
  }
}
const StyledPluginSchemaEditor = withStyles(styles)(PluginSchemaEditorBase);

const PluginSchemaEditor = (props) => {
  return (
    <ThemeWrapper>
      <StyledPluginSchemaEditor {...props} />
    </ThemeWrapper>
  );
};
(PluginSchemaEditor as any).propTypes = {
  schemas: PropTypes.array,
  onSchemaChange: PropTypes.func,
  disabled: PropTypes.func,
  actionsDropdownMap: PropTypes.array,
  schemaTitle: PropTypes.string,
  isSchemaMacro: PropTypes.func,
};
export { PluginSchemaEditor };
