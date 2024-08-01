package io.cdap.cdap.internal.ai;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.ai.spi.AIProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.artifact.AppRequest;
import java.io.IOException;

public class DefaultAIProvider implements AIProvider {

  private final Injector injector;
  private final CConfiguration cConf;
  private final AIProviderExtensionLoader extensionLoader;

  private volatile AIProvider delegate;

  @Inject
  DefaultAIProvider(Injector injector, CConfiguration cConf,
      AIProviderExtensionLoader extensionLoader) throws Exception {
    this.injector = injector;
    this.cConf = cConf;
    this.extensionLoader = extensionLoader;
    this.extensionLoader.getAll();
    // TODD: Use the AI provider name from the cConf.
    this.delegate = this.extensionLoader.get("gcp-vertexai");
    this.delegate.initialize(new DefaultAIProviderContext(cConf, "gcp-vertexai"));
  }

  @Override
  public String getName() {
    return "system";
  }

  @Override
  public String summarizeApp(AppRequest appRequest, String format) throws IOException {
    return this.delegate.summarizeApp(appRequest, format);
  }
}
