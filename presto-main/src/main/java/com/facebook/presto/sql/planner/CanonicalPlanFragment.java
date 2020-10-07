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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CanonicalPlanFragment
{
    private final PlanNode plan;
    private final CanonicalPartitioningScheme partitioningScheme;

    public CanonicalPlanFragment(PlanNode plan, CanonicalPartitioningScheme partitioningScheme)
    {
        this.plan = requireNonNull(plan, "plan is null");
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
    }

    public PlanNode getPlan()
    {
        return plan;
    }

    public CanonicalPartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    public CanonicalPlanFragment removeColumnPredicate(List<ColumnHandle> columns)
    {
        Rewriter rewriter = new Rewriter(columns);
        return new CanonicalPlanFragment(rewriteWith(rewriter, plan), partitioningScheme);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final List<ColumnHandle> columns;

        private Rewriter(List<ColumnHandle> columns)
        {
            this.columns = columns;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            if (node instanceof CanonicalTableScanNode) {
                return ((CanonicalTableScanNode) node).removeColumnPredicate(columns);
            }
            return super.visitPlan(node, context);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CanonicalPlanFragment that = (CanonicalPlanFragment) o;
        return Objects.equals(plan, that.plan) &&
                Objects.equals(partitioningScheme, that.partitioningScheme);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(plan, partitioningScheme);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("plan", plan)
                .add("partitioningScheme", partitioningScheme)
                .toString();
    }
}
