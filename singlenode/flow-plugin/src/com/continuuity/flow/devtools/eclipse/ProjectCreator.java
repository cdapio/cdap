package com.continuuity.flow.devtools.eclipse;

import com.continuuity.flow.devtools.eclipse.templates.FileTemplateConfig;
import org.eclipse.core.resources.IContainer;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.IWorkspaceRoot;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.SubProgressMonitor;
import org.eclipse.emf.codegen.jet.JETEmitter;
import org.eclipse.emf.codegen.jet.JETException;
import org.eclipse.emf.common.util.DiagnosticException;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IWorkbenchPage;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.PlatformUI;
import org.eclipse.ui.ide.IDE;
import org.osgi.framework.Bundle;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Vector;


/**
 * This class creates a Java Project in the workspace along with the referenced 
 * libraries required for a particular template. It also generates the template
 * and adds it to the created project. 
 */
public class ProjectCreator {

  private String projectName;
  private FileTemplateConfig templateConfig;
  
  /**
   * Creates a new {@link ProjectCreator} instance given the project name 
   * and a template configuration.
   * 
   * @param projectName the name of the project to be created
   * @param templateConfig the template configuration of the template to be added
   */
  public ProjectCreator(String projectName, FileTemplateConfig templateConfig) {
    this.projectName = projectName;
    this.templateConfig = templateConfig;
  }
  
  /**
   * Creates a new project in the workspace and returns the data to be 
   * inserted into the template file.
   * 
   * <p>The dependency classpaths are added as referenced libraries to the 
   * project. The error message is the message generated if any of the required 
   * library is not found.
   * 
   * @param monitor the progress monitor
   * @param dependencyClasspaths the dependency class paths to be referenced
   * @param errorMessage the error message to be added to the template
   * @return  The generated template data
   * @throws CoreException
   *         if anything goes wrong
   */
  public String generate(IProgressMonitor monitor, final Vector<IClasspathEntry>
      dependencyClasspaths, String errorMessage) throws CoreException {
    String generatedTemplateContent = new String();
    try {
      JETEmitter emitter = new JETEmitter(getTemplateFilePath(),
          getClass().getClassLoader()) {
        /* (non-Javadoc)
         * @see org.eclipse.emf.codegen.jet.JETEmitter#getClasspathEntries()
         */
        @Override
        public List<IClasspathEntry> getClasspathEntries() {
          List<IClasspathEntry> entries = super.getClasspathEntries();
          entries.addAll(dependencyClasspaths);
          return entries;
        }
      };
      emitter.setProjectName(projectName);
      emitter.addVariable("Continuuity Flow Plugin", Activator.PLUGIN_ID);
      generatedTemplateContent = emitter.generate(monitor, 
          new Object[]{templateConfig, errorMessage});
    } catch(JETException e) {
      e.printStackTrace();
      throw DiagnosticException.toCoreException(e);
    }
    return generatedTemplateContent;
  }

  /**
   * Adds a file to the project in the workspace and returns it. 
   * 
   * @param destinationContainer the relative container path in the project created
   * @param templateContent the content of the template to be added
   * @param monitor the progress monitor
   * @param noOfTicks the number of ticks allocated to the monitor
   * 
   * @return the generated template file
   * @throws CoreException
   *         if anything goes wrong.
   */
  public IFile addFileToProject(String destinationContainer, 
      String templateContent, String fileName, IProgressMonitor monitor, int noOfTicks) 
      throws CoreException {
    IWorkspaceRoot root = ResourcesPlugin.getWorkspace().getRoot();
    IResource resource = root.findMember(new Path(destinationContainer));

    if (resource == null || !resource.exists() || !(resource instanceof IContainer)) {
      throw ExceptionHandler.getNewCoreException(destinationContainer + " does not exist!", null);
    }
    
    IContainer container = (IContainer) resource;
    final IFile file = container.getFile(new Path(fileName));
    try {
      InputStream fileStream = new ByteArrayInputStream(templateContent.getBytes());
      if (file.exists()) {
        file.setContents(fileStream, true, true, 
            new SubProgressMonitor(monitor, (int) (0.3 * noOfTicks))); //30% work
      } else {
        file.create(fileStream, true, 
            new SubProgressMonitor(monitor, (int) (0.3 * noOfTicks))); //30% work
      }
      fileStream.close();
    } catch (IOException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(CoreException e) {
      e.printStackTrace();
      throw e;
    }
    
    return file;
  }
  
  /**
   * Creates a new template from the {@code templateContent}, adds it to 
   * the project created in the workspace and returns it.
   * 
   * <p>The generated template is added to the {@code destinationContainer}
   * in the project created in the workspace.
   *  
   * 
   * @param destinationContainer the relative container path in the project created
   * @param templateContent the content of the template to be added
   * @param monitor the progress monitor
   * @param noOfTicks the number of ticks allocated to the monitor
   * 
   * @return the generated template file
   * @throws CoreException
   *         if anything goes wrong.
   */
  public IFile addTemplateToProject(String destinationContainer, 
      String templateContent, String fileName, IProgressMonitor monitor, int noOfTicks) 
      throws CoreException {
    IWorkspaceRoot root = ResourcesPlugin.getWorkspace().getRoot();
    IResource resource = root.findMember(new Path(destinationContainer));
    if (!resource.exists() || !(resource instanceof IContainer)) {
      throw ExceptionHandler.getNewCoreException(destinationContainer + " does not exist!", null);
    }
    IContainer container = (IContainer) resource;
    String templateFileName = fileName;
    final IFile file = container.getFile(new Path(templateFileName));
    try {
      InputStream templateStream = new ByteArrayInputStream(templateContent.getBytes());
      if (file.exists()) {
        file.setContents(templateStream, true, true, 
            new SubProgressMonitor(monitor, (int) (0.3 * noOfTicks))); //30% work
      } else {
        file.create(templateStream, true, 
            new SubProgressMonitor(monitor, (int) (0.3 * noOfTicks))); //30% work
      }
      templateStream.close();
    } catch (IOException e) {
      e.printStackTrace();
      ExceptionHandler.throwCoreException(e.getMessage(), e);
    } catch(CoreException e) {
      e.printStackTrace();
      throw e;
    }
    
    // Delete TemporaryTemplateCreator.java file from workspace. It is generated in ProjectCreator 
    try {
      IFile jetClassFile = container.getFile(new Path("TemporaryTemplateCreator.java"));
      jetClassFile.delete(true, new SubProgressMonitor(monitor, (int) (0.2 * noOfTicks))); //20% work
    } catch(CoreException e) {
      e.printStackTrace();
      throw e;
    }
    
    return file;
  }
  
  /**
   * Opens the template file which is added to the project for editing.
   * 
   * @param file the template file to be opened
   * @param shell the shell to open the generated template
   * @param monitor the progress monitor
   * @param noOfTicks the number of ticks allocated to the monitor
   */
  public void openFile(final IFile file, Shell shell, IProgressMonitor monitor,
      int noOfTicks) {
    monitor.subTask("Opening file for editing...");
    shell.getDisplay().asyncExec(new Runnable() {
      public void run() {
        IWorkbenchPage page =
            PlatformUI.getWorkbench().getActiveWorkbenchWindow().getActivePage();
        IWorkbenchPart part = page.getActivePart();
        try {
          IDE.openEditor(page, file, true);
        } catch (PartInitException e) {
          ExceptionHandler.handleCoreException("ERROR", "Error opening template!", 
              ExceptionHandler.getNewCoreException(e.getMessage(), e));
        }
      }
    });
    monitor.worked(noOfTicks);
  }
  
  
  /**
   * Returns the template URI of the template model file
   * @return the template URI
   */
  private String getTemplateFilePath() {
    String pluginId = Activator.PLUGIN_ID;
    Bundle core = Platform.getBundle(pluginId);
    String templateDir = new String();
    String relativeDir = FileTemplateConfig.class.getPackage().getName().replace('.', '/');
    try {
      URL imageUrl = FileLocator.find(core, new Path(relativeDir), null);
      if(imageUrl == null){
        imageUrl = FileLocator.find(core, new Path("src/"+relativeDir), null);
      }
      templateDir = FileLocator.toFileURL(imageUrl).toString();
      templateDir = templateDir.substring("file:".length());
    } catch(IOException e) {
      e.printStackTrace();
    }
    return templateDir + templateConfig.getFileName();
  }

  
}
