const TEST_PIPELINE_NAME = '__UI_test_pipeline';
const TEST_PATH = '__UI_test_path';

describe('Creating a pipeline', function() {
  it('is configured correctly', function() {
    // Go to Pipelines studio
    cy.visit('/');
    cy.get('#resource-center-btn')
      .click();
    cy.contains('Create')
      .click();
    cy.url()
      .should('include', '/pipelines');

    // Add an action node, to create minimal working pipeline
    cy.get('.item')
      .contains('Conditions and Actions')
      .click();
    cy.get('.item-body-wrapper')
      .contains('File Delete')
      .click();

    // Fill out required Path input field with some test value
    cy.get('.node-configure-btn')
      .invoke('show')
      .click();
    cy.get('.form-group')
      .contains('Path')
      .parent()
      .find('input')
      .click()
      .type(TEST_PATH);

    cy.get('[data-testid=close-config-popover]')
      .click();

    // Click on Configure, toggle Instrumentation value, then Save
    cy.contains('Configure')
      .click();
    cy.get('.label-with-toggle')
      .contains('Instrumentation')
      .parent()
      .as('instrumentationDiv');
    cy.get('@instrumentationDiv')
      .contains('On');
    cy.get('@instrumentationDiv')
      .find('.toggle-switch')
      .click();
    cy.get('@instrumentationDiv')
      .contains('Off');
    cy.get('[data-testid=config-apply-close]')
      .click();

    // Name pipeline then deploy pipeline
    cy.get('.pipeline-name')
      .click();
    cy.get('#pipeline-name-input')
      .type(TEST_PIPELINE_NAME)
      .type('{enter}');
    cy.get('[data-testid=deploy-pipeline]')
      .click();

    // Do assertions
    cy.url()
      .should('include', '/view/__UI_test_pipeline');
    cy.contains(TEST_PIPELINE_NAME);
    cy.contains('FileDelete');
    cy.contains('Configure')
      .click();
    cy.contains('Pipeline config')
      .click();
    cy.get('.label-with-toggle')
      .contains('Instrumentation')
      .parent()
      .as('instrumentationDiv');
    cy.get('@instrumentationDiv')
      .contains('Off');
    cy.get('[data-testid=close-modeless]')
      .click();

    // Delete the pipeline to clean up
    cy.get('.pipeline-actions-popper')
      .click();
    cy.get('[data-testid=delete-pipeline]')
      .click();
    cy.get('[data-testid=confirmation-modal]')
      .find('.btn-primary')
      .click();

    // Assert pipeline no longer exists
    cy.contains(TEST_PIPELINE_NAME)
      .should('not.exist');
  });
});
