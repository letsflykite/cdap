/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data.configuration;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.hooks.MetricsReporterHook;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.gateway.handlers.PingHandler;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Configuration HTTP Service.
 */
public class ConfigService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigService.class);
  private final DiscoveryService discoveryService;
  private final NettyHttpService.Builder builder;
  private NettyHttpService httpService;
  private Cancellable cancellable;

  @Inject
  public ConfigService(CConfiguration cConf, DiscoveryService discoveryService,
                       @Nullable MetricsCollectionService metricsCollectionService) {
    String address = cConf.get(Constants.ConfigService.ADDRESS);
    int backlogcnxs = cConf.getInt(Constants.ConfigService.BACKLOG_CONNECTIONS, 20000);
    int execthreads = cConf.getInt(Constants.ConfigService.EXEC_THREADS, 20);
    int bossthreads = cConf.getInt(Constants.ConfigService.BOSS_THREADS, 1);
    int workerthreads = cConf.getInt(Constants.ConfigService.WORKER_THREADS, 10);
    builder = NettyHttpService.builder();

    builder.setHandlerHooks(ImmutableList.of(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.ConfigService.HTTP_HANDLER)));
    builder.setHost(address);
    builder.setConnectionBacklog(backlogcnxs);
    builder.setExecThreadPoolSize(execthreads);
    builder.setBossThreadPoolSize(bossthreads);
    builder.setWorkerThreadPoolSize(workerthreads);

    this.discoveryService = discoveryService;
    LOG.info("Configuring ConfigService " +
               ", address: " + address +
               ", backlog connections: " + backlogcnxs +
               ", execthreads: " + execthreads +
               ", bossthreads: " + bossthreads +
               ", workerthreads: " + workerthreads);
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(new ServiceLoggingContext(Constants.Logging.SYSTEM_NAME,
                                                                       Constants.Logging.COMPONENT_NAME,
                                                                       Constants.Service.CONFIG_SERVICE));
    builder.addHttpHandlers(ImmutableList.<HttpHandler>of(new PingHandler()));
    httpService = builder.build();
    LOG.info("Starting Config Service...");
    httpService.startAndWait();
    LOG.info("Started Config Service...");
    cancellable = discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return Constants.Service.CONFIG_SERVICE;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return httpService.getBindAddress();
      }
    });

    LOG.info("Config Service started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Config Service");
    cancellable.cancel();
    httpService.stopAndWait();
  }
}