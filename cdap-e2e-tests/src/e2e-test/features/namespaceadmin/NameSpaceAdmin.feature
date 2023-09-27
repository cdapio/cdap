@Namespaceadmin
Feature: NameSpaceAdmin - Validate system admin page flow

  @Namespaceadmin
  Scenario:Validate user is able to create new namespace preferences inside namespace admin
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click "preferences" tab from Configuration page for "default" Namespace
    Then Click on edit namespace preferences to set namespace preferences
    Then Set system preferences with key: "keyValue" and value: "systemPreferences1"
    Then Click on the Save & Close preferences button

  Scenario:Validate user is able to open compute profile page and select a provisioner
    Given Open Datafusion Project to configure pipeline
    Then Click on the Hamburger bar on the left panel
    Then Click on NameSpace Admin link from the menu
    Then Click on create profile button for "default" Namespace
    Then Select a provisioner: "existingDataProc" for the compute profile
    Then Click on close button of compute profile properties page