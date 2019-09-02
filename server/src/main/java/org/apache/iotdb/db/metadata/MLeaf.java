/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MLeaf extends MNode {
    private List<MNode> parents;
    private MLeaf previousSibling;
    private MLeaf nextSibling;

    public MLeaf(String name, MNode parent, TSDataType dataType, TSEncoding encoding, CompressionType type) {
        super(name, dataType, encoding, type);
        this.parents = new LinkedList<>();
        this.addParent(parent);
        this.previousSibling = null;
        this.nextSibling = null;
    }

    public boolean hasParents() {
        return !parents.isEmpty();
    }

    public boolean hasParent(MNode parent) {
        return parents.contains(parent);
    }

    public void addParent(MNode parent) {
        if (!hasParent(parent)) {
            parents.add(parent);
        }
    }

    public void deleteParent(MNode parent) {
        parents.remove(parent);
    }

    public boolean hasPreviousSibling() {
        return previousSibling != null;
    }

    public boolean hasNextSibling() {
        return nextSibling != null;
    }

    public List<MNode> getParents() {
        return parents;
    }

    public MLeaf getPreviousSibling() {
        return previousSibling;
    }

    public MLeaf getNextSibling() {
        return nextSibling;
    }

    public void setPreviousSibling(MLeaf previousSibling) {
        this.previousSibling = previousSibling;
    }

    public void setNextSibling(MLeaf nextSibling) {
        this.nextSibling = nextSibling;
    }
}
