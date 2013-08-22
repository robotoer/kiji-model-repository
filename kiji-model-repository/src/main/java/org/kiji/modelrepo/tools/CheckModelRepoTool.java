/**
 * (c) Copyright 2013 WibiData, Inc.
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

import com.google.common.base.Preconditions;

import org.kiji.common.flags.Flag;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;

/**
 * Model repository consistency check tool checks that every model in the model repository table
 * is associated with a valid artifact (a jar/war file) in the model repository.
 */
public final class CheckModelRepoTool extends BaseTool implements KijiModelRepoTool {

  @Flag(name="kiji", usage="Name of the Kiji instance housing the model repository.")
  private String mInstanceName = null;

  @Flag(name="download-and-validate-artifact",
      usage="Download and validate the model artifact. Default: true")
  private boolean mDownload = true;

  private KijiURI mInstanceURI = null;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mInstanceName == null) {
      mInstanceName = KConstants.DEFAULT_INSTANCE_URI;
    }
    mInstanceURI = KijiURI.newBuilder(mInstanceName).build();
  }

  @Override
  public String getName() {
    return MODEL_REPO_TOOL_BASE + getModelRepoToolName();
  }

  @Override
  public String getCategory() {
    return MODEL_REPO_TOOL_CATEGORY;
  }

  @Override
  public String getModelRepoToolName() {
    return "check";
  }

  @Override
  public String getDescription() {
    return "Checks model repository for consistency.";
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.size() == 0);
    final Kiji kijiInstance = Kiji.Factory.open(mInstanceURI);
    final KijiModelRepository kmr = KijiModelRepository.open(kijiInstance);
    try {
      final List<Exception> issues = kmr.checkModelLocations(mDownload);
      getPrintStream().printf("%d issues found!%n", issues.size());
      for (final Exception issue: issues) {
        getPrintStream().printf("%s%n", issue.getMessage());
      }
    } finally {
      kmr.close();
      kijiInstance.release();
    }
    return SUCCESS;
  }
}
