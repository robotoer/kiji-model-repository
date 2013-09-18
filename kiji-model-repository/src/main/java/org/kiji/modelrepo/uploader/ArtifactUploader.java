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

import org.kiji.modelrepo.ArtifactName;

/**
 * Defines an interface for uploading artifacts to another location.
 */
public interface ArtifactUploader {

  /**
   * Uploads an artifact to another location.
   * This follows similar to the Maven convention of artifact naming.
   *
   * @param artifact name of the artifact which is being uploaded.
   * @param baseURI The base URI of the storage layer.
   * @param artifactPath The local file to upload.
   * @return relative location of where the file was uploaded.
   *
   * @throws IOException if there is an exception uploading the artifact.
   */
  String uploadArtifact(
      ArtifactName artifact,
      URI baseURI,
      File artifactPath) throws IOException;
}
