package com.continuuity.flow.devtools.eclipse.ui;

import com.continuuity.flow.devtools.eclipse.ExceptionHandler;
import com.continuuity.flow.devtools.eclipse.templates.FileTemplateConfig;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.dialogs.WizardNewProjectCreationPage;

import java.io.File;
import java.util.Iterator;
import java.util.Map;


/**
 * A class that creates a new Continuuity flow wizard page. It allows setting the new
 * Continuuity flow project name, select the project template and provide the dependency path.
 * It also provides the external dependency options.
 */
public class NewProjectWizardPage extends WizardNewProjectCreationPage {

  private String selectedTemplate;
  private String extDependencyDirText;
  private String downloadDependencyDirText;
  private String javaClientLibPath;
  
  private boolean isDependencyPathValid;
  private boolean isJavaClienLibPathValid;
  
  private Map<String, FileTemplateConfig> templateConfigMap;
  private FileTemplateConfig selectedTemplateConfig;
  
  public static enum DependencyOptions {
    EXT_DEPENDENCY_NOT_DESIRED,
    DOWNLOAD_EXT_DEPENDENCIES,
    LOCATE_EXT_DEPENDENCIES
  }
  
  private DependencyOptions selectedDependencyOption;
  
  /**
   * Creates a new Continuuity flow Project Wizard Page.
   * 
   * <p> If {@code title} is null, then it won't be displayed.
   * <p> If {@code description} is null, then it won't be displayed.
   * <p> If {@code image} is null, then it won't be displayed.
   * 
   * @param pageName the name of this page
   * @param title the title of this page
   * @param description the description of this page
   * @param templateConfigMap the Map of template configuration elements 
   */
  public NewProjectWizardPage(String pageName, String title, String description, 
      Map<String, FileTemplateConfig> templateConfigMap) {
    super(pageName);
    setTitle(title);
    setDescription(description);
    this.templateConfigMap = templateConfigMap;
  }

  /* (non-Javadoc)
   * @see org.eclipse.ui.dialogs.WizardNewProjectCreationPage
   * #createControl(org.eclipse.swt.widgets.Composite)
   */
  @Override
  public void createControl(Composite parent) {
    
    super.createControl(parent);
    Composite composite = new Composite((Composite)getControl(), SWT.NULL);
    
    GridLayout layout = new GridLayout();
    layout.numColumns = 1;
    composite.setLayout(layout);
    composite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    try {
      createTemplateGroup(composite);
    } catch(CoreException e) {
      ExceptionHandler.handleCoreException("ERROR", "Template List error", e);
      getShell().close();
    }
    createCFlowJavaClientLibGroup(composite);
    createDependencyGroup(composite);
    
    composite.pack();
    getShell().setSize(getShell()
        .computeSize(parent.getSize().x, parent.getSize().y + composite.getSize().y + 10));
    setControl(composite);
    
  }
  
  /**
   * Creates the template group in the wizard page.
   * @param parent the parent {@link Composite}
   * @throws CoreException
   *         if anything goes wrong
   */
  private void createTemplateGroup(Composite parent) throws CoreException {

    Group templateGroup = new Group(parent, SWT.SHADOW_ETCHED_IN);
    
    GridLayout templateLayout = new GridLayout();
    templateLayout.numColumns = 2;
    templateLayout.marginWidth = 10;
    templateLayout.marginBottom = 7;
    templateLayout.makeColumnsEqualWidth = true;

    templateGroup.setText("Select a project template");
    templateGroup.setLayout(templateLayout);
    GridData gridData = new GridData(GridData.FILL_BOTH);
    gridData.heightHint = 175;
    templateGroup.setLayoutData(gridData);
    templateGroup.setFont(parent.getFont());
        
    // List for template names
    final List templateList = new List(templateGroup, SWT.SINGLE | SWT.BORDER | SWT.V_SCROLL);
    GridData templateGridData = new GridData(GridData.FILL_BOTH);
    templateList.setLayoutData(templateGridData);
    
    // Text Area for template description
    final Text descriptionArea = new Text(templateGroup, SWT.MULTI | SWT.READ_ONLY
        | SWT.WRAP | SWT.BORDER | SWT.V_SCROLL);
    GridData descriptionGridData = new GridData(GridData.FILL_VERTICAL);
    descriptionGridData.widthHint = 200;
    descriptionArea.setLayoutData(descriptionGridData);
    
    Iterator<FileTemplateConfig> templateIterator = templateConfigMap.values().iterator(); 
    while(templateIterator.hasNext()) {
      String templateName = templateIterator.next().getName();
      if(templateName.length() > 0) {
        templateList.add(templateName);
      }
    }
    if(templateList.getItemCount() == 0) {
      ExceptionHandler.throwCoreException("templates.xml misses the required data!", null);
    }
    templateList.addListener(SWT.Selection, new Listener() {

      public void handleEvent(Event event) {
        selectedTemplate = templateList.getItem(templateList.getSelectionIndex()).trim();
        descriptionArea.setText(getTemplateDescription());
      }
    });
    templateList.setSelection(0);
    selectedTemplate = templateList.getItem(templateList.getSelectionIndex()).trim();
    descriptionArea.setText(getTemplateDescription());
  }
  
  /**
   * Returns the modified description for the selected template
   * @return the template description
   */
  private String getTemplateDescription() {
    selectedTemplateConfig = templateConfigMap.get(selectedTemplate);
    String description = "External dependencies required: \n";
    if(selectedTemplateConfig.isExtDependencyRequired()) {
      java.util.List<String> dependencyNameList = 
          selectedTemplateConfig.getExtDependencyNames();
      for(int i = 0; i < dependencyNameList.size(); i++) {
        description += "\n" + (i + 1) + ".) " + dependencyNameList.get(i);
      }
    } else {
      description += "\nNone";
    }
    description += "\n\n" + selectedTemplateConfig.getDescription();
    return description;
  }
  
  /**
   * Creates thw Continuuity flow & data java Client Library Group in the wizard page.
   * @param parent the parent {@link Composite} 
   */
  private void createCFlowJavaClientLibGroup(Composite parent) {
    
    Group javaClientLibGroup = new Group(parent, SWT.SHADOW_ETCHED_IN);
    
    GridLayout javaClientLibLayout = new GridLayout();
    javaClientLibLayout.numColumns = 3;
    javaClientLibLayout.marginWidth = 10;
    javaClientLibLayout.marginBottom = 7;

    javaClientLibGroup.setText("Specify path to Continuuity Flow and Data fabric client library jars");
    javaClientLibGroup.setLayout(javaClientLibLayout);
    javaClientLibGroup.setLayoutData(new GridData(GridData.FILL_BOTH));
    javaClientLibGroup.setFont(parent.getFont());

    // Location Label
    Label locationLabel = new Label(javaClientLibGroup,SWT.SHADOW_IN);
    locationLabel.setText("Location:");
    
    //Add text box and browse button to the dependency path composite
    final Text javaClientLibDirText = new Text(javaClientLibGroup, SWT.SINGLE | SWT.BORDER);
    GridData dirTextGridData = new GridData(GridData.FILL_HORIZONTAL);
    javaClientLibDirText.setLayoutData(dirTextGridData);
    isJavaClienLibPathValid = false;
    
    //Add modify listener to dependencyDirText
    javaClientLibDirText.addModifyListener(new ModifyListener() {

      public void modifyText(ModifyEvent e) {
        javaClientLibPath = javaClientLibDirText.getText();
        String error = checkValidLocation(javaClientLibPath);
        error = (error == null) ? null : "Java Client Library Path " + error;
        isJavaClienLibPathValid = (error == null) ? true : false;
        reportError(error);
      }
    });
    
    final Button browseButton = new Button(javaClientLibGroup, SWT.PUSH);
    
    GridData browseGridData = new GridData(GridData.FILL_HORIZONTAL);
    browseGridData.widthHint = 95;
    browseGridData.grabExcessHorizontalSpace = false;
    browseButton.setLayoutData(browseGridData);
    browseButton.setText("Browse...");
    
    //Directory Dialog to select dependency path
    final DirectoryDialog javaClientLibDirDialog = new DirectoryDialog(getShell(), SWT.NULL);
    javaClientLibDirDialog.setText("Continuuity API Java Client Library Directory Dialog");
    javaClientLibDirDialog.setMessage("Select a directory containing the Continuuity "
                                    + "API java client libraries");
    
    //Add listener to browse button
    browseButton.addListener(SWT.Selection, new Listener() {

      public void handleEvent(Event event) {
        javaClientLibDirDialog.setFilterPath(javaClientLibDirText.getText().trim());
        javaClientLibDirText.setText(javaClientLibDirDialog.open());
      }
    });
  }
  
  
  /**
   * Creates the dependency group in the wizard page.
   * @param parent the parent {@link Composite}
   */
  private void createDependencyGroup(Composite parent) {
    
    Group dependencyGroup = new Group(parent, SWT.SHADOW_ETCHED_IN);
    
    GridLayout dependencyLayout = new GridLayout();
    dependencyLayout.numColumns = 1;
    dependencyLayout.marginWidth = 10;
    dependencyLayout.marginBottom = 7;

    dependencyGroup.setText("Specify how to handle external dependencies");
    dependencyGroup.setLayout(dependencyLayout);
    dependencyGroup.setLayoutData(new GridData(GridData.FILL_BOTH));
    dependencyGroup.setFont(parent.getFont());
    
    //Radio Buttons
    final Button[] radios = new Button[DependencyOptions.values().length];
    
    radios[DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED.ordinal()] 
        = new Button(dependencyGroup, SWT.RADIO);
    radios[DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED.ordinal()].setSelection(true);
    selectedDependencyOption = DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED;
    radios[DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED.ordinal()]
        .setText("Do not require");
    
    radios[DependencyOptions.DOWNLOAD_EXT_DEPENDENCIES.ordinal()] 
        = new Button(dependencyGroup, SWT.RADIO);
    radios[DependencyOptions.DOWNLOAD_EXT_DEPENDENCIES.ordinal()]
        .setText("Download dependency to");
    
    //New composite for dependency path selection
    final Composite downloadDependencyPathComposite = new Composite(dependencyGroup, SWT.NULL);
    
    GridLayout downloadDependencyPathLayout = new GridLayout();
    downloadDependencyPathLayout.numColumns = 2;
    downloadDependencyPathLayout.marginWidth = 10;
    downloadDependencyPathLayout.marginBottom = 7;

    downloadDependencyPathComposite.setLayout(downloadDependencyPathLayout);
    downloadDependencyPathComposite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    
    //Add text box and browse button to the dependency path composite
    final Text downloadDependencyLocText = new Text(downloadDependencyPathComposite, 
        SWT.SINGLE | SWT.BORDER);
    GridData downloadDirTextGridData = new GridData(GridData.FILL_HORIZONTAL);
    downloadDirTextGridData.widthHint = 300;
    downloadDependencyLocText.setLayoutData(downloadDirTextGridData);
    
  //Add modify listener to dependencyDirText
    downloadDependencyLocText.addModifyListener(new ModifyListener() {

      public void modifyText(ModifyEvent e) {
        if(radios[1].getSelection()){
          downloadDependencyDirText = downloadDependencyLocText.getText();
          String error = checkValidLocation(downloadDependencyDirText);
          error = (error == null) ? null : "Path where dependencies are to be downloaded " + error;
          isDependencyPathValid = (error == null) ? true : false;
          reportError(error);
        }
      }
    });
    
    final Button downloadLocBrowseButton = new Button(downloadDependencyPathComposite, SWT.PUSH);
    GridData downloadLocBrowseGridData = new GridData(GridData.FILL_HORIZONTAL);
    downloadLocBrowseGridData.widthHint = 95;
    downloadLocBrowseGridData.grabExcessHorizontalSpace = false;
    downloadLocBrowseButton.setLayoutData(downloadLocBrowseGridData);
    downloadLocBrowseButton.setText("Browse...");
    
    downloadDependencyLocText.setEnabled(false);
    downloadLocBrowseButton.setEnabled(false);
    
    //Directory Dialog to select dependency path
    final DirectoryDialog dependencyDirectoryDialog = new DirectoryDialog(getShell(), SWT.NULL);
    
    //Add listener to browse button
    downloadLocBrowseButton.addListener(SWT.Selection, new Listener() {

      public void handleEvent(Event event) {
        dependencyDirectoryDialog.setMessage("Select an output directory where you want "
                                           + "to keep the downloaded dependencies");
        dependencyDirectoryDialog.setText("Dependencies' Output Directory");
        //Set the initial filter path according to the text entered in text box
        dependencyDirectoryDialog.setFilterPath(downloadDependencyLocText.getText().trim());
        downloadDependencyLocText.setText(dependencyDirectoryDialog.open());
      }
    });
    
    radios[DependencyOptions.LOCATE_EXT_DEPENDENCIES.ordinal()] 
        = new Button(dependencyGroup, SWT.RADIO);
    radios[DependencyOptions.LOCATE_EXT_DEPENDENCIES.ordinal()].setText("Select location");
    
    //New composite for dependency path selection
    final Composite dependencyPathComposite = new Composite(dependencyGroup, SWT.NULL);
    
    GridLayout dependencyPathLayout = new GridLayout();
    dependencyPathLayout.numColumns = 2;
    dependencyPathLayout.marginWidth = 10;
    dependencyPathLayout.marginBottom = 7;

    dependencyPathComposite.setLayout(dependencyPathLayout);
    dependencyPathComposite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
    
    //Add text box and browse button to the dependency path composite
    final Text dependencyDirText = new Text(dependencyPathComposite, SWT.SINGLE | SWT.BORDER);
    GridData dirTextGridData = new GridData(GridData.FILL_HORIZONTAL);
    dirTextGridData.widthHint = 300;
    dependencyDirText.setLayoutData(dirTextGridData);
    
    //Add modify listener to dependencyDirText
    dependencyDirText.addModifyListener(new ModifyListener() {

      public void modifyText(ModifyEvent e) {
        if(radios[2].getSelection()){
          extDependencyDirText = dependencyDirText.getText();
          String error = checkValidLocation(extDependencyDirText);
          error = (error == null) ? null : "Path where external dependencies exist " + error;
          isDependencyPathValid = (error == null) ? true : false;
          reportError(error);
        }
      }
    });
        
    final Button locBrowseButton = new Button(dependencyPathComposite, SWT.PUSH);
    GridData locBrowseGridData = new GridData(GridData.FILL_HORIZONTAL);
    locBrowseGridData.widthHint = 95;
    locBrowseGridData.grabExcessHorizontalSpace = false;
    locBrowseButton.setLayoutData(locBrowseGridData);
    locBrowseButton.setText("Browse...");
    
    dependencyDirText.setEnabled(false);
    locBrowseButton.setEnabled(false);
        
    //Add listener to browse button
    locBrowseButton.addListener(SWT.Selection, new Listener() {

      public void handleEvent(Event event) {
        dependencyDirectoryDialog.setMessage("Select the directory containing "
                                           + "the external dependencies");
        dependencyDirectoryDialog.setText("External Dependencies' Input Directory");
        //Set the initial filter path according to the text entered in text box
        dependencyDirectoryDialog.setFilterPath(dependencyDirText.getText().trim());
        
        dependencyDirText.setText(dependencyDirectoryDialog.open());
      }
    });

    //create listener for radio buttons
    Listener dependencyOptionListener = new Listener() {
      
      public void handleEvent(Event event) {
        Button selectedOption = (Button)event.widget;  
        if(selectedOption.getSelection()) {
          for(int i = 0; i < radios.length; i++) {
            if(radios[i].getText().equals(selectedOption.getText())) {
              for(DependencyOptions dependencyOption : DependencyOptions.values()) {
                if(dependencyOption.ordinal() == i) {
                  selectedDependencyOption = dependencyOption;
                }
              }
            }
          }
          
          switch(selectedDependencyOption) {
            case EXT_DEPENDENCY_NOT_DESIRED:
              downloadDependencyLocText.setEnabled(false);
              downloadLocBrowseButton.setEnabled(false);
              dependencyDirText.setEnabled(false);
              locBrowseButton.setEnabled(false);
              reportError(null);
              break;
              
            case DOWNLOAD_EXT_DEPENDENCIES:
              dependencyDirText.setEnabled(false);
              locBrowseButton.setEnabled(false);
              downloadDependencyLocText.setEnabled(true);
              downloadLocBrowseButton.setEnabled(true);
              //To trigger text modified listener
              if(downloadDependencyLocText.getText().length() == 0) {
                downloadDependencyLocText.setText("");
              } else {
                downloadDependencyLocText.setText(downloadDependencyLocText.getText());
              }
              break;
              
            case LOCATE_EXT_DEPENDENCIES:
              downloadDependencyLocText.setEnabled(false);
              downloadLocBrowseButton.setEnabled(false);
              dependencyDirText.setEnabled(true);
              locBrowseButton.setEnabled(true);
              
              //To trigger text modified listener
              if(dependencyDirText.getText().length() == 0) {
                dependencyDirText.setText("");
              } else {
                dependencyDirText.setText(dependencyDirText.getText());
              }
              break;
              
            default:
              break;
          }
        }
      }
    };
    
    radios[DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED.ordinal()]
        .addListener(SWT.Selection, dependencyOptionListener);
    radios[DependencyOptions.DOWNLOAD_EXT_DEPENDENCIES.ordinal()]
        .addListener(SWT.Selection, dependencyOptionListener);
    radios[DependencyOptions.LOCATE_EXT_DEPENDENCIES.ordinal()]
        .addListener(SWT.Selection, dependencyOptionListener);
  }

  /**
   * Returns the template name selected by the user
   * 
   * @return The template name which is selected
   */
  public String getSelectedTemplateName() {
    return selectedTemplate;
  }

  /**
   * Returns the external dependency option selected by the user.
   * 
   * @return  The external dependency option which is selected
   */
  public DependencyOptions getSelectedDependencyOption() {
    return selectedDependencyOption;
  }
  
  /**
   * Returns the external dependencies' location path as entered by the user.
   * 
   * @return  The external dependencies' location path
   */
  public String getExtDependencyDirText() {
    return extDependencyDirText; 
  }
  
  /**
   * Returns the location path as entered by the user where the external 
   * dependencies are to downloaded. 
   * 
   * @return the location path where the external dependencies are to be downloaded
   */
  public String getDownloadDependencyDirText() {
    return downloadDependencyDirText;
  }

  /**
   * Returns Continuuity Flow & Data API Library location path as entered by the user.
   * 
   * @return The Continuuity Flow & Data API Library location path
   */
  public String getJavaClientLibPath() {
    return javaClientLibPath;
  }

  /**
   * Reports error to the wizard page.
   */
  private void reportError(String errorMessage) {
    setPageComplete(isWizardPageComplete());
    setErrorMessage(errorMessage);
  }
    
  /**
   * Check the validity of the directory path.
   * @param dirPath the directory path
   * @return an appropriate message if directory path is not valid
   *         or null if it is valid
   */
  private String checkValidLocation(String dirPath) {
    if(dirPath.trim().length() == 0) {
      return "cannot be empty";
    } else {
      if(!(new File(dirPath.trim())).exists()){
        return "does not exist";
      } else {
        return null;
      }
    }
  }
  
  @Override
  public void setPageComplete(boolean complete) {
    super.setPageComplete(isWizardPageComplete());
  }
  
  /**
   * @return {@code true} if wizard page is complete; {@code false} otherwise
   */
  private boolean isWizardPageComplete() {
    boolean isPageComplete = validatePage() && isJavaClienLibPathValid;
    if(getSelectedDependencyOption() != DependencyOptions.EXT_DEPENDENCY_NOT_DESIRED) {
      isPageComplete = isPageComplete && isDependencyPathValid;
    }
    return isPageComplete;
  }
  
  
}
