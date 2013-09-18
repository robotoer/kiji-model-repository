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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;

import org.kiji.modelrepo.ArtifactName;
import org.kiji.modelrepo.MavenUtils;
import org.kiji.schema.util.ProtocolVersion;

/**
 * Uploads an artifact through the Maven command. This can deploy to a host of storage layers
 * (currently supported transports are listed here:
 * <a href="http://maven.apache.org/wagon/">Maven Wagon Plugin</a>).
 *
 * <br/>
 * <b>Note:</b> If the storage layer requires any authentication, this has to be set in your
 * $M2_REPO/settings.xml. The server id must be named "kiji-model-repository". Below is an example
 * snippet of the configuration to place in your settings.xml:
 * <pre>
 *   &lt;servers&gt;
 *     &lt;server&gt;
 *       &lt;id&gt;kiji-model-repository&lt;/id&gt;
 *       &lt;username&gt;deployment&lt;/username&gt;
 *       &lt;password&gt;deployment&lt;/password&gt;
 *     &lt;/server&gt;
 *   &lt;/servers&gt;
 * </pre>
 *
 */
public class MavenArtifactUploader implements ArtifactUploader {

  private Invoker mMavenInvoker = MavenUtils.getInvoker();

  private static final String REPOSITORY_ID = "kiji-model-repository";

  @Override
  public String uploadArtifact(
      ArtifactName artifact,
      URI baseURI,
      File artifactPath) throws IOException {

    Preconditions.checkArgument(artifactPath.exists(), "Error %s does not exist!",
        artifactPath.getAbsolutePath());
    Preconditions.checkArgument(artifact.isVersionSpecified(), "Version must be specified.");
    MavenArtifactName mavenArtifact = new MavenArtifactName(artifact.toString());

    // This will upload the artifact located in artifactPath to baseUrl via
    // mvn deploy:deploy-file

    InvocationRequest request = new DefaultInvocationRequest();
    request.setGoals(Lists.newArrayList("deploy:deploy-file"));
    Properties props = new Properties();
    props.setProperty("url", baseURI.toString());
    props.setProperty("groupId", mavenArtifact.getGroupName());
    props.setProperty("artifactId", mavenArtifact.getArtifactName());
    props.setProperty("version", mavenArtifact.getVersion().toCanonicalString());
    props.setProperty("generatePom", "false");
    props.setProperty("repositoryId", REPOSITORY_ID);
    props.setProperty("file", artifactPath.getAbsolutePath());

    request.setProperties(props);

    try {
      InvocationResult result = mMavenInvoker.execute(request);
      if (result.getExitCode() != 0) {
        throw new IOException(result.getExecutionException());
      }
    } catch (MavenInvocationException mie) {
      throw new IOException(mie);
    }

    // Construct the return location.
    StringBuilder returnLocation = new StringBuilder();
    returnLocation.append(mavenArtifact.getGroupName().replace('.', '/'));
    returnLocation.append("/");
    returnLocation.append(mavenArtifact.getArtifactName().replace('.', '/'));
    returnLocation.append("/");
    returnLocation.append(mavenArtifact.getVersion().toCanonicalString());
    returnLocation.append("/");
    returnLocation.append(
        mavenArtifact.getArtifactName()
        + "-"
        + mavenArtifact.getVersion().toCanonicalString()
        + "." + Files.getFileExtension(artifactPath.getAbsolutePath()));

    return returnLocation.toString();
  }

  /**
   * Class to encapsulate Maven artifacts' names. These names are three-part:
   * group name, artifact name, version.
   * Their canonical string representation is:
   * &lt;group name&gt;.&lt;artifact name&gt;[-&lt;version&gt;].
   */
  private static class MavenArtifactName {
    private final String mArtifactName;
    private final String mGroupName;
    private ProtocolVersion mVersion;

    /**
     * Maven artifacts' names are of the form:
     * &lt;group name&gt;.&lt;artifact name&gt;[-&lt;version&gt;].
     *
     * <li>
     *   <ul> The artifact name may not contain periods or hyphens. </ul>
     *   <ul> The group name may not contain hyphens. </ul>
     *   <ul> The version must be of the form &lt;major&gt;.&lt;minor&gt;.&lt;patch&gt;. </ul>
     * </li>
     *
     * @param name of the Maven artifact to parse.
     */
    public MavenArtifactName(final String name) {
      // E.g. org.mycompany.package.artifact-1.0.0 where
      // groupName=org.mycompany.package
      // artifactName=artifact
      // version=1.0.0
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
}
