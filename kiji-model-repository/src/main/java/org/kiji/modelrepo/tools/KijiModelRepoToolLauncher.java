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

import org.kiji.delegation.Lookups;

/**
 * Similar to the KijiToolLauncher. Provides helper methods
 * to look up implementations of the {@link KijiModelRepoTool}.
 *
 */
public final class KijiModelRepoToolLauncher {

  /**
   * Unused private constructor.
   */
  private KijiModelRepoToolLauncher() {}

  /**
   * Return the tool specified by the 'toolName' argument.
   *
   * @param toolName the name of the tool to instantiate.
   * @return the KijiModelRepoTool that provides for that name, or null if none does.
   */
  public static KijiModelRepoTool getModelRepoTool(String toolName) {
    KijiModelRepoTool tool = null;

    // Iterate over available tools, searching for the one with
    // the same name as the supplied tool name argument.
    for (KijiModelRepoTool candidate : getModelRepoTools()) {
      if (toolName.equals(candidate.getModelRepoToolName())) {
        tool = candidate;
        break;
      }
    }

    return tool;
  }

  /**
   * Returns an iterable of registered model repository tool implementations.
   *
   * @return an iterable of registered model repository tool implementations.
   */
  public static Iterable<KijiModelRepoTool> getModelRepoTools() {
    return Lookups.get(KijiModelRepoTool.class);
  }
}
