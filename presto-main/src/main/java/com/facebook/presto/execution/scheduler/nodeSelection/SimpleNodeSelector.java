/*
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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionUtils.sortedNodes;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SimpleNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SimpleNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final int maxTasksPerStage;

    public SimpleNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            int maxTasksPerStage)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.maxTasksPerStage = maxTasksPerStage;
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        NodeSelection randomNodeSelection = new RandomNodeSelection(nodeMap, includeCoordinator, minCandidates, maxTasksPerStage, existingTasks);
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;

        // todo identify if sorting will cause bottleneck
        List<HostAddress> sortedCandidates = sortedNodes(nodeMap);
        OptionalInt preferredNodeCount = OptionalInt.empty();
        for (Split split : splits) {
            List<InternalNode> candidateNodes;
            switch (split.getNodeSelectionStrategy()) {
                case HARD_AFFINITY:
                    candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(sortedCandidates), includeCoordinator);
                    break;
                case SOFT_AFFINITY:
                    candidateNodes = convertToInternalNode(nodeMap, split.getPreferredNodes(sortedCandidates));
                    preferredNodeCount = OptionalInt.of(candidateNodes.size());
                    candidateNodes = ImmutableList.<InternalNode>builder().addAll(candidateNodes).addAll(randomNodeSelection.pickNodes(split)).build();
                    break;
                case NO_PREFERENCE:
                    candidateNodes = randomNodeSelection.pickNodes(split);
                    break;
                default:
                    throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", split.getNodeSelectionStrategy()));
            }

            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            InternalNode chosenNode = chooseLeastBusyNode(candidateNodes, assignmentStats::getTotalSplitCount, preferredNodeCount, maxSplitsPerNode);
            if (chosenNode == null) {
                chosenNode = chooseLeastBusyNode(candidateNodes, assignmentStats::getQueuedSplitCountForStage, preferredNodeCount, maxPendingSplitsPerTask);
            }

            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
            }
            else {
                if (split.getNodeSelectionStrategy() != HARD_AFFINITY) {
                    splitWaitingForAnyNode = true;
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
            }
        }

        ListenableFuture<?> blocked;
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }

    private static List<InternalNode> convertToInternalNode(NodeMap nodeMap, List<HostAddress> preferredNodes)
    {
        List<InternalNode> internalNodes = new ArrayList<>();
        preferredNodes.forEach(node -> internalNodes.addAll(nodeMap.getNodesByHostAndPort().get(node)));
        return ImmutableList.copyOf(internalNodes);
    }

    private static InternalNode chooseLeastBusyNode(List<InternalNode> candidateNodes, Function<InternalNode, Integer> splitCountProvider, OptionalInt preferredNodeCount, int maxSplitCount)
    {
        return candidateNodes.get(0);
//        int min = Integer.MAX_VALUE;
//        InternalNode chosenNode = null;
//        for (int i = 0; i < candidateNodes.size(); i++) {
//            InternalNode node = candidateNodes.get(i);
//            int splitCount = splitCountProvider.apply(node);
//
//            // choose the preferred node first as long as they're not busy
//            if (preferredNodeCount.isPresent() && i < preferredNodeCount.getAsInt() && splitCount < maxSplitCount) {
//                return node;
//            }
//            // fallback to choosing the least busy nodes
//            if (splitCount < min && splitCount < maxSplitCount) {
//                chosenNode = node;
//                min = splitCount;
//            }
//        }
//        return chosenNode;
    }
}
