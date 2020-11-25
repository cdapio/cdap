export interface INodeInfo {
  nodeName: string;
  nodeType: string;
}
export interface INodeIdentifier extends INodeInfo {
  nodeId: string;
}
export interface IgetNodeIDOptions {
  [key: string]: any
}

declare global {
  namespace Cypress {
    // tslint:disable-next-line: interface-name
    interface Chainable {

      open_source_panel: () => Chainable<JQuery<HTMLElement>>;
      open_transform_panel: () => Chainable<JQuery<HTMLElement>>;
      open_analytics_panel: () => Chainable<JQuery<HTMLElement>>;
      open_sink_panel: () => Chainable<JQuery<HTMLElement>>;
      open_condition_and_actions_panel: () => Chainable<JQuery<HTMLElement>>;

      /**
       * Given the nodeidentifier object returns the html element of the plugin
       *
       * @nodeObj - NodeIdentifier object to identify the node in the canvas
       */
      get_node: (nodeObj: INodeIdentifier) => Chainable<JQuery<HTMLElement>>;

      /**
       * Add node to canvas
       *
       * @nodeObj - NodeInfo object to identify the node in the side panel
       */
      add_node_to_canvas: (nodeObj: INodeInfo) => Chainable<JQuery<HTMLElement>>;

      /**
       * Get all elements at once for matching selectors
       *
       * @elements - Any number of selectors to search dom
       */
      getAll: (selectors: Array<INodeInfo>) => Chainable<Array<JQuery<HTMLElement>>>;

      /**
       * Connect two plugins. This involves connecting source endpoint to anywhere in the target node.
       *
       * @sourceNode - Source node
       * @targetNode - Target node
       * @sourceEndpoint - Source jsplumb endpoint to connect from the source node. It could be anything,
       * - endpoint on right for transform,
       * - bottom for condition node or error/alert ports
       * - in a popover for splitter plugins
       */
      connect_two_nodes: (sourceNode: INodeIdentifier, targetNode: INodeIdentifier, getSourceNodeId: (options: IgetNodeIDOptions, s: string) => string, options?: IgetNodeIDOptions) => Chainable<any>;

      /**
       * Moves a node based on selector to X,Y co-ordinates
       *
       * @nodeName - Name of the node to move
       * @nodeType - Type of the node
       * @nodeId - unique identifier to indentify the node,
       * @toX - Position X to move the node
       * @toY - Position Y to move the node
       *
       * The final selector that will be generated to find the node will be of the form
       * [data-cy="plugin-node-${nodeName}-${nodeType}-${nodeId}"]
       * */
      move_node: (nodeObj: INodeIdentifier | string, toX: number, toY: number) => Chainable<JQuery<any>>;

      /**
       * Selects everything between these two nodes.
       */
      select_from_to: (from: INodeIdentifier, to: INodeIdentifier) => Chainable<any>;

      /**
       * Select a specific connection give source and target node
       */
      select_connection: (from: INodeIdentifier, to: INodeIdentifier) => Chainable<JQuery<any>>;
      /**
       * Creates a simple BQ source -> Wrangler Transform -> BQ Sink pipeline.
       *
       * This is a dumb command to be reused whenever we want to create a basic pipeline
       * to test pipeline canvas actions.
       */
      create_simple_pipeline: () => Chainable<any>;

      /**
       * Create a relatively complex pipeline with joiner and condition nodes.
       *
       * This is not a logically valid pipeline. This is purely to test pipeline actions
       */
      create_complex_pipeline: () => Chainable<any>;

      /**
       * Get pipeline config.
       */
      get_pipeline_json: () => Chainable<any>;

      /**
       * Get a specific stage info from pipeline export config
       */
      get_pipeline_stage_json: (stageName: string) => Chainable<any>;

      /**
       * Uploads a pipeline json from fixtures to input file element.
       *
       * @fileName - Name of the file from fixture folder including extension.
       * @selector - css selector to query for the input[type="file"] element.
      */
      upload_pipeline: (filename: string, selector: string) => Chainable<JQuery<HTMLElement>>;

      /**
       * Generic file upload using react-dropzone
       */
      upload: (file: any, fileName: string, fileType: string) => Chainable<any>;
      /**
       * Open plugin node property modal
       *
       * @nodeObj - NodeIdentifier object to identify the node in the canvas
       */
      open_node_property: (nodeObj: INodeIdentifier) => Chainable<JQuery<HTMLElement>>;

      /**
       * Close node property modal
       */
      close_node_property: () => Chainable<JQuery<HTMLElement>>;
      
      /** 
       * Uploads a pipeline json from fixtures to input file element.
       *
       * @headers - Any request headers to be passed.
       * @pipelineJson - pipeline draft that needs to be uploaded.
      */
      upload_draft_via_api: (headers: any, pipelineJson: any) => Chainable<Request>; 
      
      /**
       * Cleans up pipelines once expecuting a specific test. This is to remove state
       *  from individual tests.
       *
       * @headers - Any request headers to be passed.
       * @pipelineName - name of the pipeline to be deleted.
      */
      cleanup_pipelines: (headers: any, pipelineName: string) => Chainable<Request>;

      /**
       * Command to check if wrangler is up and running and if not starts wrangler
       * before running any wrangler related tests.
       */
      start_wrangler: (headers: any) => Chainable<any>;

      /**
       * Fills up the create connection form for GCS
       */
      fill_GCS_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test GCS connection
       */
      test_GCS_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a GCS connection
       */
      create_GCS_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;


      /**
      * Fills up the create connection form for BIGQUERY
      */
      fill_BIGQUERY_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test BIGQUERY connection
       */
      test_BIGQUERY_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a BigQuery connection
       */
      create_BIGQUERY_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;


      /**
      * Fills up the create connection form for SPANNER
      */
      fill_SPANNER_connection_create_form: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Test SPANNER connection
       */
      test_SPANNER_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Creates a SPANNER connection
       */
      create_SPANNER_connection: (connectionId: string, projectId?: string, serviceAccountPath?: string) => void;

      /**
       * Utility to add runtime arguments at specific key
       */
      add_runtime_args_row_with_value: (row: number, key: string, value: string) => void;

      /**
       * Utility to add runtime arguments for existing key-value in runtime arguments
       */
      update_runtime_args_row: (row: number, key: string, value: string, macro?: boolean) => void;

      /**
       * Assertion to validate a runtime argument row
       */
      assert_runtime_args_row: (row: number, key: string, value: string, macro?: boolean) => void;
      compareSnapshot: (s: string) => any;

      /**
       * Uploads a plugin json from fixtures to input file element.
       *
       * @fileName - Name of the file from fixture folder including extension.
       * @selector - data-cy selector to query for the input[type="file"] element.
      */
      upload_plugin_json: (filename: string, selector: string) => Chainable<JQuery<HTMLElement>>;

      /**
       * Cleans up secure keys after executing the tests. This is to remove state
       *  from individual tests.
       *
       * @headers - Any request headers to be passed.
       * @secureKeyName - name of the secure key to be deleted.
      */
      cleanup_secure_key: (headers: any, secureKeyName: string) => Chainable<Request>;

      /**
       *  Deletes specified artifact via REST API
       *  @headers - Any request headers to be passed.
       *  @artifactName - Name of artifact to be deleted.
       *  @version - Version of the artifact to be deleted.
       */

      delete_artifact_via_api: (headers: any, artifactName: string, version: string) => Chainable<Request>;

      fit_pipeline_to_screen: () => Chainable<JQuery<HTMLElement>>;
      pipeline_clean_up_graph_control: () => Chainable<JQuery<HTMLElement>>;

    }
    // tslint:disable-next-line: interface-name
    interface Window {
      File: any;
      DataTransfer: any;
      sessionStorage: any;
      onbeforeunload: any;
      CDAP_CONFIG: any;
      ace: any;
    }
  }
}
