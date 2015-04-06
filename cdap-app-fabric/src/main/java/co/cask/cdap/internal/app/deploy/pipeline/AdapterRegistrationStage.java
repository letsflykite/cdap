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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.app.store.Store;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import com.google.common.reflect.TypeToken;

/**
 * Adds a configured adapter to the store.
 */
public class AdapterRegistrationStage extends AbstractStage<AdapterSpecification> {
  private final Store store;
  private final Id.Namespace namespace;

  public AdapterRegistrationStage(Id.Namespace namespace, Store store) {
    super(TypeToken.of(AdapterSpecification.class));
    this.store = store;
    this.namespace = namespace;
  }

  @Override
  public void process(AdapterSpecification input) throws Exception {
    store.addAdapter(namespace, input);
    emit(input);
  }
}