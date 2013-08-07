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

package org.kiji.modelrepo.packager;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import com.google.common.base.Preconditions;

/**
 * A concrete packager that generates a war file to be deployed in any standard
 * web container.
 *
 */
public class WarPackager implements Packager {

  private static final String WAR_DEPS_DIR = "WEB-INF/lib/";

  @Override
  public void generateArtifact(File outputFile, List<File> dependencies) throws IOException {
    Preconditions.checkNotNull(outputFile);
    Preconditions.checkArgument(outputFile.getName().endsWith(".war"));

    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    JarOutputStream target = new JarOutputStream(new FileOutputStream(outputFile), manifest);

    if (dependencies != null) {
      JarEntry entry = new JarEntry(WAR_DEPS_DIR);
      target.putNextEntry(entry);
      for (File dep : dependencies) {
        add(dep, target);
      }
      target.closeEntry();
    }

    target.close();
  }

  /**
   * Adds the given dependency file to the current war file under the WEB-INF/lib folder.
   *
   * @param source is the dependency to add.
   * @param target is the war's file handle.
   * @throws IOException if there is a problem writing the file into the war.
   */
  private void add(File source, JarOutputStream target) throws IOException {
    BufferedInputStream in = null;
    try {
      JarEntry entry = new JarEntry(WAR_DEPS_DIR + source.getName());
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      in = new BufferedInputStream(new FileInputStream(source));

      byte[] buffer = new byte[1024];
      int count = in.read(buffer);
      while (count >= 0) {
        target.write(buffer, 0, count);
        count = in.read(buffer);
      }
      target.closeEntry();
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
