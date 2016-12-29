package it.unipr.aotlab.mapreduce;

import it.unipr.aotlab.actodes.actor.Binder;
import it.unipr.aotlab.actodes.actor.Case;
import it.unipr.aotlab.actodes.actor.KillableBehavior;
import it.unipr.aotlab.actodes.filtering.MessagePattern;

/**
 *
 * The {@code QWorkerer} class defines a behavior that waits for messages
 * from a {@code Master} actor until it receives a {@code KILL} message.
 *
 * When it happens it kills itself.
 *
 * @see Master
 *
**/

public final class Worker extends KillableBehavior
{
  private static final long serialVersionUID = 1L;

  /**
   * {@inheritDoc}
   *
   * @param v  the arguments:
   *
   * the number of messages.
   *
  **/
  @Override
  public void initialize(final Binder b, final Object[] v)
  {
    b.bind(KILLPATTERN, this.killCase);

    Case c = (m) -> {
      send(m, ((String) m.getContent()).length());

      return null;
    };

    b.bind(MessagePattern.NOCONSTRAINTS, c);
  }
}



