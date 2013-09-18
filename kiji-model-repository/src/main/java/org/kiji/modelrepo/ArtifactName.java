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

package org.kiji.modelrepo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import org.kiji.schema.util.ProtocolVersion;

/**
 * Class to represent model repository artifact names, which are naturally serialized as
 * [String,ProtocolVersion] ordered pairs.
 */
public final class ArtifactName {
  /**
   * Java package naming convention (sans numerics).
   * Names must be composed of the lowercase alphabet or underscore.
   * There must be at least one period and periods may not occur consecutively.
   * E.g.
   * org.kiji.fake.project
   * org.kiji.fake._project
   * kiji.something_
   */
  private static final Pattern NAME_PATTERN =
      Pattern.compile("([a-z]|_)+\\.(([a-z]|_)+(\\.)+)*([a-z]|_)+");
  private final String mName;
  private final ProtocolVersion mVersion;

  /**
   * Maven artifacts' names sans version are of the form
   * &lt;package&gt;.&lt;identifier&gt;,
   * where &lt;package&gt; and &lt;identifier&gt; are
   * composed of lowercase alphabetic characters and underscore.
   *
   * @param name of the Maven artifact to parse, sans version.
   * @param version of the Maven artifact. Null means unspecified.
   */
  public ArtifactName(
      final String name,
      final ProtocolVersion version) {
    this(String.format("%s-%s",
        name,
        (null == version) ? "" : version.toCanonicalString()));
  }

  /**
   * Maven artifacts' names are of the form:
   * &lt;package&gt;.&lt;identifier&gt;[-&lt;version&gt;],
   * where &lt;package&gt; and &lt;identifier&gt; are
   * composed of lowercase alphabetic characters and underscore.
   * The version must be of the form &lt;major&gt;.&lt;minor&gt;.&lt;patch&gt;.
   *
   * @param name of the Maven artifact to parse.
   */
  public ArtifactName(final String name) {
    // E.g. org.mycompany.fake.artifact-1.0.0 where
    // package=org.mycompany.package
    // identifier=artifact
    // version=1.0.0
    final int hyphenPosition = name.indexOf("-");
    if (0 < hyphenPosition) {
      // TODO: Determine if this is the right version to put in.
      // Maven uses x.y.z-qualifier, whereas ProtocolVersion doesn't support qualifiers.
      mVersion = ProtocolVersion.parse(name.substring(hyphenPosition + 1));
      mName = name.substring(0, hyphenPosition);
    } else {
      mVersion = null;
      mName = name;
    }
    final Matcher nameMatcher = NAME_PATTERN.matcher(mName);
    Preconditions.checkArgument(nameMatcher.matches(),
        "Artifact name must be of the form: <package>.<identifier>[-<version>]%n"
            + "The <package> and <identifier> strings must be lowercase alphabetic or underscore.");
  }

  /**
   * Gets the name of the artifact in the model repository.
   *
   * @return artifact name.
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the version of the artifact in the model repository.
   * Returns null is version is unspecified.
   *
   * @return artifactVersion.
   */
  public ProtocolVersion getVersion() {
    return mVersion;
  }

  /**
   * Checks whether the version has been specified.
   *
   * @return true iff version specified.
   */
  public boolean isVersionSpecified() {
    if (null == mVersion) {
      return false;
    }
    return true;
  }

  /**
   * Returns the canonical form of the artifact name and version if specified.
   *
   * @return canonical artifact name.
   */
  public String toString() {
    return String.format("%s-%s",
        mName,
        (null == mVersion) ? "" : mVersion.toCanonicalString());
  }
}
