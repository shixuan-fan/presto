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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    public static class AggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final boolean useSystemMemory;
        private boolean closed;
        private final JsonCodec<Split> splitCodec;

        public AggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, Step step, List<AccumulatorFactory> accumulatorFactories, boolean useSystemMemory, JsonCodec<Split> splitCodec)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.useSystemMemory = useSystemMemory;
            this.splitCodec = splitCodec;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            return new AggregationOperator(operatorContext, step, accumulatorFactories, useSystemMemory, splitCodec);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new AggregationOperatorFactory(operatorId, planNodeId, step, accumulatorFactories, useSystemMemory, splitCodec);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Aggregator> aggregates;
    private final boolean useSystemMemory;
    private final Step step;
    private final JsonCodec<Split> splitCodec;

    private State state = State.NEEDS_INPUT;
    private Split split = null;

    public AggregationOperator(OperatorContext operatorContext, Step step, List<AccumulatorFactory> accumulatorFactories, boolean useSystemMemory, JsonCodec<Split> splitCodec)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(AggregationOperator.class.getSimpleName());
        this.userMemoryContext = operatorContext.localUserMemoryContext();
        this.useSystemMemory = useSystemMemory;
        this.step = step;
        this.splitCodec = splitCodec;

        requireNonNull(step, "step is null");

        // wrapper each function with an aggregator
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AccumulatorFactory accumulatorFactory : accumulatorFactories) {
            builder.add(new Aggregator(accumulatorFactory, step));
        }
        aggregates = builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public void close()
    {
        userMemoryContext.setBytes(0);
        systemMemoryContext.close();
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");

        long memorySize = 0;
        for (Aggregator aggregate : aggregates) {
//            if (step == PARTIAL) {
//                if (split == null && page.getPositionCount() > 0) {
//                    Block splitBlock = page.getBlock(page.getChannelCount() - 1);
//                    split = splitCodec.fromJson(splitBlock.getSlice(0, 0, splitBlock.getSliceLength(0)).getBytes());
//                }
//                Block[] dataBlocks = new Block[page.getChannelCount() - 1];
//                for (int i = 0; i < page.getChannelCount() - 1; i++) {
//                    dataBlocks[i] = page.getBlock(i);
//                }
//                page = new Page(page.getPositionCount(), dataBlocks);
//            }
            aggregate.processPage(page);
            memorySize += aggregate.getEstimatedSize();
        }
        if (useSystemMemory) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            userMemoryContext.setBytes(memorySize);
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        // project results into output blocks
        List<Type> types = aggregates.stream().map(Aggregator::getType).collect(toImmutableList());

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder pageBuilder = new PageBuilder(1, types);

        pageBuilder.declarePosition();
        for (int i = 0; i < aggregates.size(); i++) {
            Aggregator aggregator = aggregates.get(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
            aggregator.evaluate(blockBuilder);
        }

        state = State.FINISHED;
        Page page = pageBuilder.build();
        if (split == null) {
            return page;
        }
        return page.appendColumn(RunLengthEncodedBlock.create(VARBINARY, wrappedBuffer(splitCodec.toJsonBytes(split)), page.getPositionCount()));
    }
}
