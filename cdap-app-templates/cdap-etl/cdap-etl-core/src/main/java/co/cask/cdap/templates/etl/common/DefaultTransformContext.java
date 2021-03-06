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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.templates.etl.api.StageSpecification;
import co.cask.cdap.templates.etl.api.TransformContext;

import java.util.Map;

/**
 * Context for the Transform Stage.
 */
public class DefaultTransformContext implements TransformContext {
  private final StageSpecification specification;
  private final Map<String, String> runtimeArgs;

  public DefaultTransformContext(StageSpecification specification, Map<String, String> runtimeArgs) {
    this.specification = specification;
    this.runtimeArgs = runtimeArgs;
  }

  @Override
  public StageSpecification getSpecification() {
    return specification;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }
}
