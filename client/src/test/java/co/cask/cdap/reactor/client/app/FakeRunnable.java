package co.cask.cdap.reactor.client.app;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;

/**
 * Fake no-op TwillRunnable.
 */
public final class FakeRunnable extends AbstractTwillRunnable {
  public static final String NAME = "fakeRunnable";

  @Override
  public void initialize(TwillContext context) {
    context.announce(NAME, 12352);
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(NAME)
      .noConfigs()
      .build();
  }

  @Override
  public void run() {
    // no-op
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }
}
