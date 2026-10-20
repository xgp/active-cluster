package com.github.xgp.cluster;

/**
 * <code>Service</code> represents some service of some kind with a simple start/stop lifecycle.
 *
 * @version $Revision: 1.2 $
 */
public interface Service {

  /**
   * Starts the service. Calling it on a started service has no effect.
   *
   * @throws ClusterException if the service cannot be started
   */
  public void start() throws ClusterException;

  /**
   * Stops the service. Calling it on a stopped service has no effect.
   *
   * @throws ClusterException if the service cannot be stopped cleanly
   */
  public void stop() throws ClusterException;
}
