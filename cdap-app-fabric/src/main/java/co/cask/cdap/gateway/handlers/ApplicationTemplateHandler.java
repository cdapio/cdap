/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.templates.plugins.PluginClass;
import co.cask.cdap.api.templates.plugins.PluginInfo;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.adapter.AdapterService;
import co.cask.cdap.internal.app.runtime.adapter.ApplicationTemplateInfo;
import co.cask.cdap.internal.app.runtime.adapter.PluginRepository;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.template.ApplicationTemplateDetail;
import co.cask.cdap.proto.template.ApplicationTemplateMeta;
import co.cask.cdap.proto.template.PluginDetail;
import co.cask.cdap.proto.template.PluginMeta;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * HTTP Handler to server the "/templates" endpoint for querying information about application template and
 * plugins information.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/templates")
public class ApplicationTemplateHandler extends AbstractHttpHandler {

  private final AdapterService adapterService;
  private final PluginRepository pluginRepository;

  @Inject
  ApplicationTemplateHandler(AdapterService adapterService, PluginRepository pluginRepository) {
    this.adapterService = adapterService;
    this.pluginRepository = pluginRepository;
  }

  /**
   * Gets all templates available in the system.
   * Response is a list of {@link ApplicationTemplateMeta} objects.
   */
  @GET
  public void getTemplates(HttpRequest request, HttpResponder responder) {
    Collection<ApplicationTemplateMeta> templates = Collections2.transform(
      adapterService.getAllTemplates(), new Function<ApplicationTemplateInfo, ApplicationTemplateMeta>() {
      @Override
      public ApplicationTemplateMeta apply(ApplicationTemplateInfo info) {
        return createTemplateMeta(info);
      }
    });
    responder.sendJson(HttpResponseStatus.OK, templates);
  }

  /**
   * Gets details about the given template.
   * Response is a {@link ApplicationTemplateDetail} object.
   *
   * @param templateId the template id to query for
   */
  @Path("/{template-id}")
  @GET
  public void getTemplate(HttpRequest request, HttpResponder responder,
                          @PathParam("template-id") String templateId) throws NotFoundException {
    ApplicationTemplateInfo templateInfo = adapterService.getApplicationTemplateInfo(templateId);
    if (templateInfo == null) {
      throw new NotFoundException(Id.ApplicationTemplate.from(templateId));
    }

    Map<PluginInfo, Collection<PluginClass>> plugins = pluginRepository.getPlugins(templateInfo.getName());
    Set<String> extensions = Sets.newTreeSet();

    for (Map.Entry<PluginInfo, Collection<PluginClass>> entry : plugins.entrySet()) {
      for (PluginClass pluginClass : entry.getValue()) {
        extensions.add(pluginClass.getType());
      }
    }

    ApplicationTemplateDetail templateDetail = new ApplicationTemplateDetail(templateInfo.getName(),
                                                                             templateInfo.getDescription(),
                                                                             templateInfo.getProgramType(),
                                                                             extensions);
    responder.sendJson(HttpResponseStatus.OK, templateDetail);
  }

  /**
   * Gets all plugins information for the given template and plugin type.
   * Response is a list of {@link PluginMeta} objects.
   *
   * @param templateId the template id to query for
   * @param pluginType the plugin type to query for
   */
  @Path("/{template-id}/extensions/{plugin-type}")
  @GET
  public void getPlugins(HttpRequest request, HttpResponder responder,
                         @PathParam("template-id") String templateId,
                         @PathParam("plugin-type") final String pluginType) throws NotFoundException {

    ApplicationTemplateInfo templateInfo = adapterService.getApplicationTemplateInfo(templateId);
    if (templateInfo == null) {
      throw new NotFoundException(Id.ApplicationTemplate.from(templateId));
    }

    List<PluginMeta> pluginDetails = Lists.newArrayList();
    for (Map.Entry<PluginInfo, Collection<PluginClass>> entry : pluginRepository.getPlugins(templateId).entrySet()) {
      // Filter by plugin type, followed by creation of PluginDetail
      pluginDetails.addAll(
        Collections2.transform(
          Collections2.filter(entry.getValue(), createPluginFilter(pluginType)),
          createPluginMetaTransform(createTemplateMeta(templateInfo), entry.getKey())
        )
      );
    }
    responder.sendJson(HttpResponseStatus.OK, pluginDetails);
  }

  /**
   * Gets details about the given plugin.
   * Response is a list of {@link PluginDetail} objects.
   *
   * @param templateId the template id to query for
   * @param pluginType the plugin type to query for
   * @param pluginName the plugin name to query for
   */
  @Path("/{template-id}/extensions/{plugin-type}/plugins/{plugin-name}")
  @GET
  public void getPlugin(HttpRequest request, HttpResponder responder,
                        @PathParam("template-id") String templateId,
                        @PathParam("plugin-type") final String pluginType,
                        @PathParam("plugin-name") final String pluginName) throws NotFoundException {

    ApplicationTemplateInfo templateInfo = adapterService.getApplicationTemplateInfo(templateId);
    if (templateInfo == null) {
      throw new NotFoundException(Id.ApplicationTemplate.from(templateId));
    }

    // Result is a list because for the same type and name, it can appear in multiple jar files, each with different
    // PluginVersion
    List<PluginDetail> pluginDetails = Lists.newArrayList();
    for (Map.Entry<PluginInfo, Collection<PluginClass>> entry : pluginRepository.getPlugins(templateId).entrySet()) {
      // Filter by plugin type and name, followed by creation of PluginDetail
      pluginDetails.addAll(
        Collections2.transform(
          Collections2.filter(entry.getValue(), createPluginFilter(pluginType, pluginName)),
          createPluginDetailTransform(createTemplateMeta(templateInfo), entry.getKey())
        )
      );
    }
    responder.sendJson(HttpResponseStatus.OK, pluginDetails);
  }

  private ApplicationTemplateMeta createTemplateMeta(ApplicationTemplateInfo info) {
    return new ApplicationTemplateMeta(info.getName(), info.getDescription(), info.getProgramType());
  }

  /**
   * Returns a {@link Predicate} for matching {@link PluginClass} by the given plugin type.
   */
  private Predicate<PluginClass> createPluginFilter(String pluginType) {
    return createPluginFilter(pluginType, null);
  }

  /**
   * Returns a {@link Predicate} for matching {@link PluginClass} by the given plugin type and name.
   */
  private Predicate<PluginClass> createPluginFilter(final String pluginType, @Nullable final String pluginName) {
    return new Predicate<PluginClass>() {
      @Override
      public boolean apply(PluginClass pluginClass) {
        boolean typeMatched = pluginClass.getType().equals(pluginType);
        return pluginName == null ? typeMatched : typeMatched && pluginClass.getName().equals(pluginName);
      }
    };
  }

  /**
   * Returns a {@link Function} for transforming {@link PluginClass} into {@link PluginMeta}.
   */
  private Function<PluginClass, PluginMeta> createPluginMetaTransform(final ApplicationTemplateMeta template,
                                                                      final PluginInfo pluginInfo) {
    return new Function<PluginClass, PluginMeta>() {
      @Override
      public PluginMeta apply(PluginClass pluginClass) {
        return new PluginMeta(template, pluginInfo, pluginClass);
      }
    };
  }

  /**
   * Returns a {@link Function} for transforming {@link PluginClass} into {@link PluginDetail}.
   */
  private Function<PluginClass, PluginDetail> createPluginDetailTransform(final ApplicationTemplateMeta template,
                                                                          final PluginInfo pluginInfo) {
    return new Function<PluginClass, PluginDetail>() {
      @Override
      public PluginDetail apply(PluginClass pluginClass) {
        return new PluginDetail(template, pluginInfo, pluginClass);
      }
    };
  }
}
