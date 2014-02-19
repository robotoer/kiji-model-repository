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

import java.net.URI;
import java.util.List;

import com.google.common.base.Preconditions;

import org.kiji.common.flags.Flag;
import org.kiji.modelrepo.KijiModelRepository;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.BaseTool;

/**
 * Implementation of the model repository installation tool. This will create a new model
 * repository table in the specified instance.
 *
 * Initialize model repo in specified instance with the base URI of the repository storage layer:
 * <pre>
 *   kiji model-repo init --kiji=kiji://.env/default http://repo.url:1234/releases
 * </pre>
 */
public final class InitModelRepoTool extends BaseTool implements KijiModelRepoTool {

  @Flag(name = "kiji", usage = "Name of the Kiji instance housing the model repository.")
  private String mInstanceName = null;

  private KijiURI mInstanceURI = null;

  /** The default instance to use for housing the model repo table and meta information. **/
  private static final String DEFAULT_INSTANCE_URI = KConstants.DEFAULT_INSTANCE_URI;

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mInstanceName == null) {
      mInstanceName = DEFAULT_INSTANCE_URI;
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
    return "init";
  }

  @Override
  public String getDescription() {
    return "Creates a new model repository.";
  }

  @Override
  public String getUsageString() {
    return
        "Usage:\n"
        + "    kiji model-repo init --kiji=<kiji-uri> http://repo.url:1234/releases\n"
        + "\n"
        + "Example:\n"
        + "  Initialize model repo in specified instance with the base URI of "
        + "the repository storage layer:\n"
        + "    kiji model-repo init --kiji=kiji://.env/default http://repo.url:1234/releases\n";
  }

  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    Preconditions.checkArgument(nonFlagArgs.size() == 1,
      "Must pass only a base-uri as a non flag argument.");
    String baseURI = nonFlagArgs.get(0);
    // Simply check that the URI is valid.
    URI parsedURI = URI.create(baseURI);
    Kiji kijiInstance = Kiji.Factory.open(mInstanceURI);
    try {
      KijiModelRepository.install(kijiInstance, parsedURI);
    } finally {
      kijiInstance.release();
    }
    return SUCCESS;
  }
}
