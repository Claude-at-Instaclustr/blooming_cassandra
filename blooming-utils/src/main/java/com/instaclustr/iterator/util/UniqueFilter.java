/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.instaclustr.iterator.util;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A Filter that filters out duplicate values.
 */
public class UniqueFilter<T> implements Predicate<T> {

    /** The set of objects already seen */
    protected Set<T> seen = new HashSet<>();

    @Override
    public boolean test(T o) {
        boolean retval = !seen.contains(o);
        if (retval) {
            seen.add(o);
        }
        return retval;
    }

}
