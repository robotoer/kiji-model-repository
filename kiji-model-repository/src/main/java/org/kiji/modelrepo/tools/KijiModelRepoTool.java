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

import org.kiji.schema.tools.KijiTool;

/**
 * Base interface to be implemented by command-line tools that are launched through
 * the <tt>bin/kiji model-repo</tt> script.
 *
 * <p>To register a tool to use with <tt>bin/kiji model-repo</tt>, you must also put the complete
 * name of the implementing class in a resource in your jar file at:
 * <tt>META-INF/services/org.kiji.schema.tools.KijiModelRepoTool</tt>. You may publish multiple
 * tools in this way; put each class name on its own line in this file. You can put
 * this file in <tt>src/main/resources/</tt> in your Maven project, and it will be
 * incorporated into your jar.</p>
 */
public interface KijiModelRepoTool extends KijiTool {

  /**
   * Returns the name of the model repository tool name. This is the name of the tool
   * that the user will type after having typed <tt>/bin/kiji model-repo</tt>.
   *
   * @return The name of the model repository tool.
   */
  String getModelRepoToolName();
}
