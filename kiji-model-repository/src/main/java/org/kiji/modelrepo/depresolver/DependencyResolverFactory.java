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

package org.kiji.modelrepo.depresolver;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Factory class that returns a {@link DependencyResolver} given a string that
 * identifies the type of resolver to return.
 *
 */
public final class DependencyResolverFactory {

  private static final Map<String, DependencyResolver> RESOLVERS = Maps.newHashMap();

  static {
    RESOLVERS.put("raw", new RawDependencyResolver());
    RESOLVERS.put("maven", new MavenDependencyResolver());
  }

  /**
   * Dummy constructor.
   */
  private DependencyResolverFactory() {
  }

  /**
   * Returns the proper dependency resolver given the type of resolver requested or null
   * if no such matching resolver exists.
   *
   * @param resolverType is the type of the resolver to return.
   * @return the proper dependency resolver given the type of resolver requested or null
   *         if no such matching resolver exists.
   */
  public static DependencyResolver getResolver(String resolverType) {
    return RESOLVERS.get(resolverType);
  }
}
