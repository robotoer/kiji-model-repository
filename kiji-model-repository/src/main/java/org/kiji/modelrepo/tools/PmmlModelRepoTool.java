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

import java.util.List;

import org.kiji.common.flags.Flag;
import org.kiji.schema.tools.BaseTool;

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
 *       [--attach=False]
 * </pre>
 */
public final class PmmlModelRepoTool extends BaseTool implements KijiModelRepoTool{
  @Flag(
      name = "table",
      usage = "URI of the Kiji table containing the desired model predictors and results."
  )
  private String mTableName = null;

  @Flag(
      name = "model-file",
      usage = "Path to the trained PMML model file."
  )
  private String mModelFile = null;

  @Flag(
      name = "model-name",
      usage = "Name of the model in the trained PMML model file."
  )
  private String mModelName = null;

  @Flag(
      name = "predictor-column",
      usage = "Name of the column containing predictor records in the form \"family:column\"."
  )
  private String mPredictorColumn = null;

  @Flag(
      name = "result-column",
      usage = "Name of the column to write result records to in the form \"family:column\"."
  )
  private String mResultColumn = null;

  @Flag(
      name = "result-record-name",
      usage = "Name of the Avro record that will contain results."
  )
  private String mResultRecordName = null;

  @Flag(
      name = "validate",
      usage = "Validate the provided values against the provided Kiji table. Default is true."
  )
  private boolean mValidate = true;

  @Flag(
      name = "attach",
      usage = "Attach the created model container to the provided result column. Default is false."
  )
  private boolean mAttach = false;

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    return 0;
  }

  @Override
  public String getModelRepoToolName() {
    return "pmml";
  }

  @Override
  public String getName() {
    return MODEL_REPO_TOOL_BASE + getModelRepoToolName();
  }

  @Override
  public String getDescription() {
    return
        "Usage:\n"
        + "";
  }

  @Override
  public String getCategory() {
    return MODEL_REPO_TOOL_CATEGORY;
  }
}
