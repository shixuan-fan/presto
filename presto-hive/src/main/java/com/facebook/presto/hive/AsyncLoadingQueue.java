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
package com.facebook.presto.hive;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Throwables.propagateIfPossible;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;

public class AsyncLoadingQueue
        implements Iterator<HivePartitionMetadata>
{
    private final AtomicInteger loaded = new AtomicInteger();
    private final AtomicInteger polled = new AtomicInteger();
    private final List<SettableFuture<HivePartitionMetadata>> partitions = new ArrayList<>();

    public AsyncLoadingQueue(
            List<HivePartition> partitionNames,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            Function<List<HivePartition>, List<HivePartitionMetadata>> loadFunction,
            Executor executor,
            int maxLoadConcurrency)
    {
        BoundedExecutor boundedExecutor = new BoundedExecutor(executor, maxLoadConcurrency);
        for (int i = 0; i < partitionNames.size(); i++) {
            partitions.add(SettableFuture.create());
        }

        if (partitionNames.size() == 1) {
            partitions.get(0).set(new HivePartitionMetadata(partitionNames.get(0), Optional.empty(), ImmutableMap.of()));
        }
        else {
            for (List<HivePartition> partitionNamesInBatch : partitionExponentially(partitionNames, minPartitionBatchSize, maxPartitionBatchSize)) {
                boundedExecutor.execute(() -> {
                    List<HivePartitionMetadata> partitionMetadata;
                    try {
                        partitionMetadata = loadFunction.apply(partitionNamesInBatch);
                    }
                    catch (RuntimeException e) {
                        for (int i = 0; i < partitionNamesInBatch.size(); i++) {
                            partitions.get(loaded.getAndIncrement()).setException(e);
                        }
                        return;
                    }

                    for (HivePartitionMetadata partition : partitionMetadata) {
                        partitions.get(loaded.getAndIncrement()).set(partition);
                    }
                });
            }
        }
    }

    @Override
    public boolean hasNext()
    {
        return polled.get() < partitions.size();
    }

    @Override
    public HivePartitionMetadata next()
    {
        int pollIndex = polled.getAndIncrement();
        if (pollIndex < partitions.size()) {
            try {
                return partitions.get(pollIndex).get();
            }
            catch (InterruptedException e) {
                currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                propagateIfPossible(e.getCause(), PrestoException.class);
                throw new RuntimeException(e.getCause());
            }
        }
        return null;
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(List<T> values, int minBatchSize, int maxBatchSize)
    {
        return () -> new AbstractIterator<List<T>>()
        {
            private int currentSize = minBatchSize;
            private final Iterator<T> iterator = values.iterator();

            @Override
            protected List<T> computeNext()
            {
                if (!iterator.hasNext()) {
                    return endOfData();
                }

                int count = 0;
                ImmutableList.Builder<T> builder = ImmutableList.builder();
                while (iterator.hasNext() && count < currentSize) {
                    builder.add(iterator.next());
                    ++count;
                }

                currentSize = min(maxBatchSize, currentSize * 2);
                return builder.build();
            }
        };
    }
}
