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

import org.kiji.schema.tools.BaseTool;

/**
 * The base implementation of the model repository tool suite. This will respond to
 * <tt>bin/kiji model-repo</tt> and in the absence of any further arguments, will print the
 * list of available tools within the model repository context.
 *
 * Tools belonging to the model repository suite must subclass and implement the
 * {@link KijiModelRepoTool} interface.
 *
 */
public class BaseModelRepoTool extends BaseTool {

  @Override
  public final String getCategory() {
    return "Model Repository";
  }

  @Override
  public String getDescription() {
    return "Model Repository Tool Suite";
  }

  @Override
  public final String getName() {
    return "model-repo";
  }

  @Override
  protected final int run(List<String> toolArgs) throws Exception {
    // Get the tool corresponding with the first argument in the list of non-tool args.
    if (toolArgs.isEmpty()) {
      printHelp();
      return FAILURE;
    }
    String subToolName = toolArgs.get(0);
    KijiModelRepoTool subTool = KijiModelRepoToolLauncher.getModelRepoTool(subToolName);
    if (subTool == null) {
      System.out.println("Unknown tool " + subToolName);
      return FAILURE;
    } else {
      return subTool.toolMain(toolArgs.subList(0, toolArgs.size()));
    }
  }

  /**
   * Prints a simple help message outlining the various tools belonging to the model
   * repository suite. All model repository tools are implementations of the
   * {@link KijiModelRepoTool}
   *
   */
  private void printHelp() {
    System.out.println("The list of available model repository tools:");
    for (KijiModelRepoTool tool : KijiModelRepoToolLauncher.getModelRepoTools()) {
      System.out.println(tool.getName() + " - " + tool.getDescription());
    }
  }
}
