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

package co.cask.cdap.common.exception;

import co.cask.cdap.proto.Id;

/**
 * Thrown when a namespace is not found in CDAP.
 */
public class NamespaceNotFoundException extends ObjectNotFoundException {

  private final Id.Namespace namespace;

  @Deprecated
  public NamespaceNotFoundException(String namespaceId) {
    super("namespace", namespaceId);
    this.namespace = Id.Namespace.from(namespaceId);
  }

  public NamespaceNotFoundException(Id.Namespace id) {
    super(id);
    this.namespace = id;

  }

  public Id.Namespace getNamespace() {
    return namespace;
  }

  @Deprecated
  public String getNamespaceId() {
    return namespace.getId();
  }
}
