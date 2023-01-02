package io.cdap.cdap.gateway.handlers;


import com.google.common.base.Throwables;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.security.AuditDetail;
import io.cdap.cdap.common.security.AuditPolicy;
import io.cdap.cdap.gateway.handlers.util.AbstractAppFabricHttpHandler;
import io.cdap.cdap.proto.SourceControlMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link io.cdap.http.HttpHandler} for managing source control operations.
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}/source-control")
public class SourceControlServiceHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SourceControlServiceHandler.class);
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final HashMap<NamespaceId, SourceControlMeta> sourceControlRepo = new HashMap<>();

  @Inject
  SourceControlServiceHandler(NamespaceQueryAdmin namespaceQueryAdmin) {
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @PUT
  @Path("/initialise")
  @AuditPolicy(AuditDetail.REQUEST_BODY)
  public void initialiseSourceControl(FullHttpRequest request, HttpResponder responder,
                                      @PathParam("namespace-id") final String namespaceId)
    throws BadRequestException, NamespaceNotFoundException, AccessException {
    NamespaceId ns = validateNamespace(namespaceId);
    StringBuilder s = new StringBuilder();
    try {
      SourceControlMeta meta = parseBody(request, SourceControlMeta.class);
      if (meta == null) {
        throw new Exception("Source control information can't be null");
      }
      // This assumes running on Linux.
      String parentDirName = "/tmp/" + UUID.randomUUID();
      CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(
        meta.getPersonalAccessToken(), "");
      File f = new File(parentDirName + "/.git");
      FileRepository localRepo = new FileRepository(f);
      LOG.info("Created local repo: " + f.getAbsolutePath());
      Git git = new Git(localRepo);
      LsRemoteCommand cmd = git.lsRemote()
        .setRemote(meta.getRepositoryURL())
        .setCredentialsProvider(credentialsProvider)
        .setHeads(false)
        .setTags(true)
        .setTimeout(10);
      Collection<Ref> collection = cmd.call();

      if (collection != null) {
        for (Ref ref : collection) {
          s.append(ref.getName());
        }
      }
      git.close();
      sourceControlRepo.put(ns, meta);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Invalid json object provided in request body.");
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    responder.sendString(HttpResponseStatus.CREATED, "Got refs: " + s);
  }

  @GET
  @Path("discover")
  public void discoverPipelines(FullHttpRequest request, HttpResponder responder,
                                @PathParam("namespace-id") final String namespaceId)
    throws BadRequestException, NamespaceNotFoundException {
    NamespaceId ns = validateNamespace(namespaceId);
    SourceControlMeta meta = sourceControlRepo.get(ns);
    if (meta == null) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, "Namespace not found: " + ns.getNamespace());
      return;
    }

    // This assumes running on Linux.
    String parentDirName = "/tmp/" + UUID.randomUUID();
    CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(
      meta.getPersonalAccessToken(), "");
    File f = new File(parentDirName);
    try {
      Git git = Git.cloneRepository()
        .setCredentialsProvider(credentialsProvider)
        .setURI(meta.getRepositoryURL())
        .setDirectory(f)
        .setBranchesToClone(Collections.singleton(("refs/heads/develop")))
        .setBranch("refs/heads/develop")
        .call();
      git.close();
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Errors: " + e.getMessage());
      return;
    }
    StringBuilder s = new StringBuilder();
    for (File ff : Objects.requireNonNull(f.listFiles())) {
      if (!ff.isDirectory()) {
        s.append(ff.getName());
      }
    }
    responder.sendString(HttpResponseStatus.OK, "Got files: " + s);
  }


  private NamespaceId validateNamespace(@Nullable String namespace)
    throws BadRequestException, NamespaceNotFoundException, AccessException {

    if (namespace == null) {
      throw new BadRequestException("Path parameter namespace-id cannot be empty");
    }

    NamespaceId namespaceId;
    try {
      namespaceId = new NamespaceId(namespace);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(String.format("Invalid namespace '%s'", namespace), e);
    }

    try {
      if (!namespaceId.equals(NamespaceId.SYSTEM)) {
        namespaceQueryAdmin.get(namespaceId);
      }
    } catch (NamespaceNotFoundException | AccessException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP calls to interact with namespaces.
      // In AppFabricServer, NamespaceAdmin is bound to DefaultNamespaceAdmin, which interacts directly with the MDS.
      // Hence, this exception will never be thrown
      throw Throwables.propagate(e);
    }
    return namespaceId;
  }
}
