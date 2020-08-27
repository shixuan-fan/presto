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
package com.facebook.presto.operator;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.FragmentCacheKey;

import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.sql.planner.LocalExecutionPlanner.fragmentCache;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DriverSplitCacheOperator
        implements Operator
{
    public static class DriverSplitCacheOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNode planNode;
        private final JsonCodec<Split> splitCodec;

        private boolean closed;

        public DriverSplitCacheOperatorFactory(
                int operatorId,
                PlanNode planNode,
                JsonCodec<Split> splitCodec)
        {
            this.operatorId = operatorId;
            this.planNode = requireNonNull(planNode, "planNodeId is null");
            this.splitCodec = splitCodec;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new DriverSplitCacheOperator(
                    driverContext.addOperatorContext(operatorId, planNode.getId(), DriverSplitCacheOperator.class.getSimpleName()),
                    planNode,
                    splitCodec);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("duplicate() is not supported for DynamicFilterSourceOperatorFactory");
        }
    }

    private final OperatorContext context;
    private final PlanNode planNode;
    private final JsonCodec<Split> splitCodec;

    private Split split;
    private boolean finished;
    private List<Page> pages = new LinkedList<>();
    private Page current;
    private FragmentCacheKey cacheKey;

    private DriverSplitCacheOperator(
            OperatorContext context,
            PlanNode planNode,
            JsonCodec<Split> splitCodec)
    {
        this.context = requireNonNull(context, "context is null");
        this.planNode = planNode;
        this.splitCodec = splitCodec;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() shouldn't not be called after finish()");
        current = page;
//        Block splitBlock = page.getBlock(page.getChannelCount() - 1);
        if (cacheKey == null) {
//            cacheKey = new FragmentCacheKey(planNode, splitCodec.fromJson(splitBlock.getSlice(0, 0, splitBlock.getSliceLength(0)).getBytes()));
            cacheKey = new FragmentCacheKey(planNode, split);
        }
        if (!fragmentCache.containsKey(cacheKey)) {
            pages.add(page);
        }
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;
        if (cacheKey != null && !fragmentCache.containsKey(cacheKey)) {
            fragmentCache.put(cacheKey, pages);
        }
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    public void setSplit(Split split)
    {
        this.split = split;
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }
}
