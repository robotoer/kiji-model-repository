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

package org.kiji.modelrepo.uploader;

import com.google.common.base.Preconditions;

import org.kiji.modelrepo.ArtifactName;
import org.kiji.schema.util.ProtocolVersion;

/**
 * Class to encapsulate Maven artifacts' names. These names are three-part:
 * group name, artifact name, version.
 * Their canonical string representation is:
 * &lt;group name&gt;.&lt;artifact name&gt;[-&lt;version&gt;].
 */
public class MavenArtifactName {
  private final String mArtifactName;
  private final String mGroupName;
  private ProtocolVersion mVersion;

  /**
   * Maven artifacts' names are of the form:
   * &lt;group name&gt;.&lt;artifact name&gt;[-&lt;version&gt;].
   *
   * <li>
   * <ul>
   * The artifact name may not contain periods or hyphens.
   * </ul>
   * <ul>
   * The group name may not contain hyphens.
   * </ul>
   * <ul>
   * The version must be of the form &lt;major&gt;.&lt;minor&gt;.&lt;patch&gt;.
   * </ul>
   * </li>
   *
   * @param artifactName of the Maven artifact to parse.
   */
  public MavenArtifactName(final ArtifactName artifactName) {
    // E.g. org.mycompany.package.artifact-1.0.0 where
    // groupName=org.mycompany.package
    // artifactName=artifact
    // version=1.0.0
    final String name = artifactName.getFullyQualifiedName();
    final int hyphenPosition = name.indexOf("-");
    final String groupAndArtifactName;
    if (0 < hyphenPosition) {
      // TODO: Determine if this is the right version to put in.
      // Maven uses x.y.z-qualifier, whereas ProtocolVersion doesn't support qualifiers.
      mVersion = ProtocolVersion.parse(name.substring(hyphenPosition + 1));
      groupAndArtifactName = name.substring(0, hyphenPosition);
    } else {
      mVersion = null;
      groupAndArtifactName = name;
    }
    final int lastPeriodPosition = groupAndArtifactName.lastIndexOf(".");
    Preconditions.checkArgument(lastPeriodPosition >= 0,
        "Artifact must specify valid group name and artifact name of the form"
            + "<group name>.<artifact name>[-<version>]");
    mGroupName = groupAndArtifactName.substring(0, lastPeriodPosition);
    mArtifactName = groupAndArtifactName.substring(lastPeriodPosition + 1);
    Preconditions.checkArgument(mGroupName.length() > 0, "Group name must be nonempty string.");
    Preconditions.checkArgument(mArtifactName.length() > 0,
        "Artifact name must be nonempty string.");
  }

  /**
   * Gets the artifact name within it's group.
   *
   * @return artifact name.
   */
  public String getArtifactName() {
    return mArtifactName;
  }

  /**
   * Gets the artifact's group name.
   *
   * @return group name.
   */
  public String getGroupName() {
    return mGroupName;
  }

  /**
   * Gets the artifact's version.
   *
   * @return version
   */
  public ProtocolVersion getVersion() {
    return mVersion;
  }
}
