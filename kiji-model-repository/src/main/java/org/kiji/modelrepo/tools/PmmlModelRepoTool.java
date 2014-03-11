/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.modelrepo.tools;

/**
 * Implementation of the pmml model repository tool.
 * This tool will create a model container for a Pmml model family.
 *
 * Create a new model without deploying it from an existing Pmml trained model:
 * <pre>
 *   kiji model-repo pmml \
 *       --table=kiji://.env/default/table \
 *       --model-file=file:///path/to/pmml \
 *       [--model-name=pmmlModelName] \
 *       --predictor-column=model:predictor \
 *       --result-column=model:result \
 *       --result-record-name=MyScore \
 *       [--validate=True] \
 *       [--deploy=False]
 * </pre>
 */
public class PmmlModelRepoTool {
}
