package com.github.xgp.cluster;

/**
 * <code>Service</code> represents some service of some kind with a simple start/stop lifecycle.
 *
 * @version $Revision: 1.2 $
 */
public interface Service {

  /**
   * Called to start the service
   *
   * @throws ClusterException
   */
  public void start() throws ClusterException;

  /**
   * Called to shutdown the service
   *
   * @throws ClusterException
   */
  public void stop() throws ClusterException;
}
