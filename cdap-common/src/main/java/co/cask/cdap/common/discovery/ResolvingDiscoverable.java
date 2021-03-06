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
package co.cask.cdap.common.discovery;

import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Discoverable that resolves 0.0.0.0 to a routable interface.
 */
public class ResolvingDiscoverable implements Discoverable {

  private static final Logger LOG = LoggerFactory.getLogger(ResolvingDiscoverable.class);

  private final Discoverable discoverable;

  private ResolvingDiscoverable(Discoverable discoverable) {
    this.discoverable = discoverable;
  }

  public static ResolvingDiscoverable of(Discoverable discoverable) {
    return new ResolvingDiscoverable(discoverable);
  }

  @Override
  public String getName() {
    return discoverable.getName();
  }

  @Override
  public InetSocketAddress getSocketAddress() {
    return resolve(discoverable.getSocketAddress());
  }

  private InetSocketAddress resolve(InetSocketAddress bindAddress) {
      if (bindAddress.getAddress().isAnyLocalAddress()) {
        try {
          return new InetSocketAddress(InetAddress.getLocalHost().getHostName(), bindAddress.getPort());
        } catch (UnknownHostException e) {
          LOG.warn("Unable to resolve localhost", e);
        }
      }
    return bindAddress;
  }
}
