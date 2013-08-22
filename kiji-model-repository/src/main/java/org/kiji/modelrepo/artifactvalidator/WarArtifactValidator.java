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

package org.kiji.modelrepo.artifactvalidator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Validator to check if file is a war.
 */
public class WarArtifactValidator implements ArtifactValidator {

  /**
   * Validate a war/jar file by parsing it and enumerating it's zip entries.
   *
   * @param artifactFile war/jar file to enumerate
   * @return true iff war/jar file was parsed unexceptionally
   */
  @Override
  public boolean isValid(final File artifactFile) {
    try {
      final JarFile targetWarFile = new JarFile(artifactFile);
      final Enumeration<JarEntry> entries = targetWarFile.entries();
      // Scan through the entries of this jar to be sure that it is a valid jar.
      while (entries.hasMoreElements()) {
        entries.nextElement();
      }
      targetWarFile.close();
    } catch (IOException e) {
      return false;
    } catch (SecurityException se) {
      return false;
    }
    return true;
  }

  /**
   * Validate war/jar input stream by checking that the file is prefixed by 0x4b50.
   *
   * @param artifactStream war/jar input stream
   * @return true iff war/jar file is prefixed by 0x4b50.
   */
  public boolean isValid(final InputStream artifactStream) {
    final int firstByte, secondByte;
    try {
      firstByte = artifactStream.read();
      secondByte = artifactStream.read();
    } catch (final IOException ioe) {
      return false;
    }
    return (80 == firstByte) && (75 == secondByte);
  }
}
