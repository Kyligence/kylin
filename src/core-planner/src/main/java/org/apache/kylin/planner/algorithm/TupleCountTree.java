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

package org.apache.kylin.planner.algorithm;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.planner.plans.TreeNode;
import org.apache.kylin.planner.util.BitUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TupleCountTree {

    private final Node root;
    private final Map<BitSet, Node> mapping;

    public TupleCountTree(BitSet rootBits, long ceiling) {
        this(new Node(rootBits, ceiling), Maps.newHashMap());
    }

    private TupleCountTree(Node root, Map<BitSet, Node> mapping) {
        this.root = root;
        this.mapping = mapping;
    }

    public void add(BitSet cuboid, long count) {
        if (mapping.containsKey(cuboid)) {
            // already exists
            return;
        }

        if (root.count <= count) {
            return;
        }

        Node node = new Node(cuboid, count);
        mapping.put(cuboid, node);

        Node parent = Objects.requireNonNull(findBestParent(cuboid, root));
        parent.addChild(node);
        adjustTree(node, root);
    }

    public void addAll(Map<BitSet, Long> countMap) {
        if (countMap == null) {
            return;
        }
        countMap.forEach(this::add);
    }

    public long findBestCost(BitSet cuboid) {
        // match exactly or find best parent
        Node best = mapping.getOrDefault(cuboid, findBestParent(cuboid, root));
        return Objects.requireNonNull(best).count;
    }

    public TupleCountTree copy() {
        return new TupleCountTree(root.copy(), Maps.newHashMap(mapping));
    }

    private Node findBestParent(BitSet cuboid, Node node) {
        if (!node.isRoot() && !BitUtil.isChild(node.cuboid, cuboid)) {
            return null;
        }

        Node best = node;
        for (Node c : node.getChildren()) {
            Node b = findBestParent(cuboid, c);
            if (b != null && b.count < best.count) {
                best = b;
            }
        }
        return best;
    }

    private void adjustTree(Node a, Node b) {
        if (isChild(a, b) && a.count < b.getParent().count) {
            a.addChild(b);
            // if b, no need to handle children
            return;
        }

        // if not b, then b's children
        // we may modify b' children when traversing 
        for (Node c : Lists.newArrayList(b.getChildren())) {
            adjustTree(a, c);
        }
    }

    private boolean isChild(Node node, Node child) {
        if (node.isRoot())
            return true;

        if (child.isRoot())
            return false;

        return BitUtil.isChild(node.cuboid, child.cuboid);
    }

    private static class Node extends TreeNode<Node> {
        private final BitSet cuboid;
        private final long count;

        private Node(BitSet cuboid, long count) {
            this.cuboid = cuboid;
            this.count = count;
            this.children = Lists.newLinkedList();
        }

        @Override
        public boolean isRoot() {
            return cuboid.nextSetBit(0) < 0;
        }

        private Node copy() {
            Node n = new Node(cuboid, count);
            for (Node c : children) {
                n.addChild(c.copy());
            }
            return n;
        }

        private void addChild(Node node) {
            if (count < node.count) {
                // Child's tuple count should never be greater than parent's.
                return;
            }

            if (node.parent != null) {
                node.parent.removeChild(node);
            }

            node.parent = this;
            children.add(node);
        }

        private void removeChild(Node node) {
            if (node == null) {
                return;
            }
            children.remove(node);
        }

        private Node getParent() {
            return parent;
        }

        private List<Node> getChildren() {
            return children;
        }
    }
}
