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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class CanonicalTableScanNode
        extends PlanNode
{
    private final CanonicalTableHandle table;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final List<VariableReferenceExpression> outputVariables;

    public CanonicalTableScanNode(
            PlanNodeId id,
            CanonicalTableHandle table,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        super(id);
        this.table = requireNonNull(table, "table is null");
        this.outputVariables = unmodifiableList(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = unmodifiableMap(new HashMap<>(requireNonNull(assignments, "assignments is null")));
        checkArgument(assignments.keySet().containsAll(outputVariables), "assignments does not cover all of outputs");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return emptyList();
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    public CanonicalTableHandle getTable()
    {
        return table;
    }

    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    public CanonicalTableScanNode removeColumnPredicate(List<ColumnHandle> columns)
    {
        return new CanonicalTableScanNode(super.getId(), table.removeColumnPredicate(columns), outputVariables, assignments);
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
        CanonicalTableScanNode that = (CanonicalTableScanNode) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(assignments, that.assignments) &&
                Objects.equals(outputVariables, that.outputVariables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, assignments, outputVariables);
    }

    public static class CanonicalTableHandle
    {
        private final ConnectorId connectorId;
        private final ConnectorTableHandle connectorHandle;

        private final Optional<Object> layoutIdentifier;

        public static CanonicalTableHandle getCanonicalTableHandle(TableHandle tableHandle)
        {
            return new CanonicalTableHandle(tableHandle.getConnectorId(), tableHandle.getConnectorHandle(), tableHandle.getLayout().map(ConnectorTableLayoutHandle::getIdentifier));
        }

        private CanonicalTableHandle(
                ConnectorId connectorId,
                ConnectorTableHandle connectorHandle,
                Optional<Object> layoutIdentifier)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
            this.layoutIdentifier = requireNonNull(layoutIdentifier, "layoutIdentifier is null");
        }

        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public ConnectorTableHandle getConnectorHandle()
        {
            return connectorHandle;
        }

        public Optional<Object> getLayoutIdentifier()
        {
            return layoutIdentifier;
        }

        public CanonicalTableHandle removeColumnPredicate(List<ColumnHandle> columns)
        {
            if (!layoutIdentifier.isPresent()) {
                return this;
            }

            TupleDomain<Subfield> domainPredicate = (TupleDomain<Subfield>) ((Map<String, Object>) layoutIdentifier.get()).get("domainPredicate");
            if (!domainPredicate.getColumnDomains().isPresent()) {
                return this;
            }

            Set<Subfield> subfields = columns.stream()
                    .map(ColumnHandle::toSubfield)
                    .collect(Collectors.toSet());
            List<ColumnDomain<Subfield>> columnDomains = domainPredicate.getColumnDomains().get().stream()
                    .filter(columnDomain -> !subfields.contains(columnDomain.getColumn()))
                    .collect(Collectors.toList());
            Map<String, Object> processedLayoutIdentifier = new HashMap<>(((Map<String, Object>) layoutIdentifier.get()));
            processedLayoutIdentifier.put("domainPredicate", TupleDomain.fromColumnDomains(Optional.of(columnDomains)));
            return new CanonicalTableHandle(connectorId, connectorHandle, Optional.of(processedLayoutIdentifier));
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
            CanonicalTableHandle that = (CanonicalTableHandle) o;
            return Objects.equals(connectorId, that.connectorId) &&
                    Objects.equals(connectorHandle, that.connectorHandle) &&
                    Objects.equals(layoutIdentifier, that.layoutIdentifier);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, connectorHandle, layoutIdentifier);
        }
    }
}
