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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.PushdownFilterResult;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.scalar.TryFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionDomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariableResolver;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.isNewOptimizerEnabled;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.metadata.TableLayoutResult.computeEnforced;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.iterative.rule.PreconditionRules.checkRulesAreFiredBeforeAddExchangesRule;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PickTableLayout
{
    private final Metadata metadata;
    private final SqlParser parser;
    private final ExpressionDomainTranslator domainTranslator;

    public PickTableLayout(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde()));
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                checkRulesAreFiredBeforeAddExchangesRule(),
                pickTableLayoutForPredicate(),
                pickTableLayoutWithoutPredicate());
    }

    public PickTableLayoutForPredicate pickTableLayoutForPredicate()
    {
        return new PickTableLayoutForPredicate(metadata, parser, domainTranslator);
    }

    public PickTableLayoutWithoutPredicate pickTableLayoutWithoutPredicate()
    {
        return new PickTableLayoutWithoutPredicate(metadata);
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;
        private final SqlParser parser;
        private final ExpressionDomainTranslator domainTranslator;

        private PickTableLayoutForPredicate(Metadata metadata, SqlParser parser, ExpressionDomainTranslator domainTranslator)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.parser = requireNonNull(parser, "parser is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        }

        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                tableScan().capturedAs(TABLE_SCAN)));

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isNewOptimizerEnabled(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            context.getSession().getSessionLogger().log(() -> "PickTableLayoutForPredicate starts");
            TableScanNode tableScan = captures.get(TABLE_SCAN);

            PlanNode rewritten = pushPredicateIntoTableScan(
                    tableScan,
                    filterNode.getPredicate(),
                    false,
                    context.getSession(),
                    context.getVariableAllocator().getTypes(),
                    context.getIdAllocator(),
                    metadata,
                    parser,
                    domainTranslator);

            Result result;
            if (arePlansSame(filterNode, tableScan, rewritten)) {
                result = Result.empty();
            }

            result = Result.ofPlanNode(rewritten);
            context.getSession().getSessionLogger().log(() -> "PickTableLayoutForPredicate ends");
            return result;
        }

        private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
        {
            if (!(rewritten instanceof FilterNode)) {
                return false;
            }

            FilterNode rewrittenFilter = (FilterNode) rewritten;
            if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
                return false;
            }

            if (!(rewrittenFilter.getSource() instanceof TableScanNode)) {
                return false;
            }

            TableScanNode rewrittenTableScan = (TableScanNode) rewrittenFilter.getSource();

            if (!tableScan.getTable().equals(rewrittenTableScan.getTable())) {
                return false;
            }

            return Objects.equals(tableScan.getCurrentConstraint(), rewrittenTableScan.getCurrentConstraint())
                    && Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint());
        }
    }

    private static final class PickTableLayoutWithoutPredicate
            implements Rule<TableScanNode>
    {
        private final Metadata metadata;

        private PickTableLayoutWithoutPredicate(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        private static final Pattern<TableScanNode> PATTERN = tableScan();

        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isNewOptimizerEnabled(session);
        }

        @Override
        public Result apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            context.getSession().getSessionLogger().log(() -> "PickTableLayoutWithoutPredicate starts");
            TableHandle tableHandle = tableScanNode.getTable();
            if (tableHandle.getLayout().isPresent()) {
                return Result.empty();
            }

            Session session = context.getSession();
            Result result;
            if (metadata.isPushdownFilterSupported(session, tableHandle)) {
                PushdownFilterResult pushdownFilterResult = metadata.pushdownFilter(session, tableHandle, TRUE_CONSTANT);
                if (pushdownFilterResult.getLayout().getPredicate().isNone()) {
                    return Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), tableScanNode.getOutputVariables(), ImmutableList.of()));
                }

                result = Result.ofPlanNode(new TableScanNode(
                        tableScanNode.getId(),
                        pushdownFilterResult.getLayout().getNewTableHandle(),
                        tableScanNode.getOutputVariables(),
                        tableScanNode.getAssignments(),
                        pushdownFilterResult.getLayout().getPredicate(),
                        TupleDomain.all()));
            }
            else {
                TableLayoutResult layout = metadata.getLayout(
                        session,
                        tableHandle,
                        Constraint.alwaysTrue(),
                        Optional.of(tableScanNode.getOutputVariables().stream()
                                .map(variable -> tableScanNode.getAssignments().get(variable))
                                .collect(toImmutableSet())));

                if (layout.getLayout().getPredicate().isNone()) {
                    result = Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), tableScanNode.getOutputVariables(), ImmutableList.of()));
                }

                result = Result.ofPlanNode(new TableScanNode(
                        tableScanNode.getId(),
                        layout.getLayout().getNewTableHandle(),
                        tableScanNode.getOutputVariables(),
                        tableScanNode.getAssignments(),
                        layout.getLayout().getPredicate(),
                        TupleDomain.all()));
            }

            context.getSession().getSessionLogger().log(() -> "PickTableLayoutWithoutPredicate ends");
            return result;
        }
    }

    /**
     * @param predicate can be a RowExpression or an OriginalExpression. The method will handle both cases.
     * Once Expression is migrated to RowExpression in PickTableLayout, the method should only support RowExpression.
     */
    public static PlanNode pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser parser,
            ExpressionDomainTranslator domainTranslator)
    {
        DomainTranslator translator = new RowExpressionDomainTranslator(metadata);

        if (!metadata.isPushdownFilterSupported(session, node.getTable())) {
            if (isExpression(predicate)) {
                return pushPredicateIntoTableScan(node, castToExpression(predicate), pruneWithPredicateExpression, session, types, idAllocator, metadata, parser, domainTranslator);
            }
            return pushPredicateIntoTableScan(node, predicate, pruneWithPredicateExpression, session, idAllocator, metadata, translator);
        }

        // filter pushdown; to be replaced by ConnectorPlanOptimizer
        RowExpression expression;
        if (isExpression(predicate)) {
            Map<NodeRef<Expression>, Type> predicateTypes = getExpressionTypes(
                    session,
                    metadata,
                    parser,
                    types,
                    castToExpression(predicate),
                    emptyList(),
                    WarningCollector.NOOP,
                    false);

            expression = SqlToRowExpressionTranslator.translate(castToExpression(predicate), predicateTypes, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), session);
        }
        else {
            expression = predicate;
        }

        // optimize rowExpression and return ValuesNode if false. e.g. 0=1 => false, 1>2 => false, (a = 1 and a = 2) => false
        DomainTranslator.ExtractionResult<VariableReferenceExpression> translatedExpression = translator.fromPredicate(session.toConnectorSession(), expression, BASIC_COLUMN_EXTRACTOR);
        if (translatedExpression.getTupleDomain().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping = node.getAssignments().entrySet().stream()
                .collect(toImmutableBiMap(
                        Map.Entry::getKey,
                        entry -> new VariableReferenceExpression(getColumnName(session, metadata, node.getTable(), entry.getValue()), entry.getKey().getType())));
        PushdownFilterResult pushdownFilterResult = metadata.pushdownFilter(session, node.getTable(), replaceExpression(expression, symbolToColumnMapping));

        TableLayout layout = pushdownFilterResult.getLayout();
        if (layout.getPredicate().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                layout.getNewTableHandle(),
                node.getOutputVariables(),
                node.getAssignments(),
                layout.getPredicate(),
                TupleDomain.all());

        RowExpression unenforcedFilter = pushdownFilterResult.getUnenforcedFilter();
        if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
            return new FilterNode(idAllocator.getNextId(), tableScan, replaceExpression(unenforcedFilter, symbolToColumnMapping.inverse()));
        }

        return tableScan;
    }

    /**
     * For Expression {@param predicate}
     */
    @Deprecated
    private static PlanNode pushPredicateIntoTableScan(
            TableScanNode node,
            Expression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser parser,
            ExpressionDomainTranslator domainTranslator)
    {
        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);
        ExpressionDomainTranslator.ExtractionResult decomposedPredicate = ExpressionDomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(variableName -> node.getAssignments().entrySet().stream().collect(toImmutableMap(entry -> entry.getKey().getName(), Map.Entry::getValue)).get(variableName))
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint<ColumnHandle> constraint;
        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluatorForExpression evaluator = new LayoutConstraintEvaluatorForExpression(
                    metadata,
                    parser,
                    session,
                    types,
                    node.getAssignments(),
                    combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(column -> assignments.containsKey(column) ? assignments.get(column).getName() : null))));
            constraint = new Constraint<>(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint<>(newDomain);
        }
        if (constraint.getSummary().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        // Layouts will be returned in order of the connector's preference
        TableLayoutResult layout = metadata.getLayout(
                session,
                node.getTable(),
                constraint,
                Optional.of(node.getOutputVariables().stream()
                        .map(variable -> node.getAssignments().get(variable))
                        .collect(toImmutableSet())));

        if (layout.getLayout().getPredicate().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                layout.getLayout().getNewTableHandle(),
                node.getOutputVariables(),
                node.getAssignments(),
                layout.getLayout().getPredicate(),
                computeEnforced(newDomain, layout.getUnenforcedConstraint()));

        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        Expression resultingPredicate = combineConjuncts(
                domainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(column -> assignments.get(column).getName())),
                filterNonDeterministicConjuncts(predicate),
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return new FilterNode(idAllocator.getNextId(), tableScan, castToRowExpression(resultingPredicate));
        }
        return tableScan;
    }

    /**
     * For RowExpression {@param predicate}
     */
    private static PlanNode pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            DomainTranslator domainTranslator)
    {
        // don't include non-deterministic predicates
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionManager()),
                new FunctionResolution(metadata.getFunctionManager()),
                metadata.getFunctionManager());
        RowExpression deterministicPredicate = logicalRowExpressions.filterDeterministicConjuncts(predicate);
        DomainTranslator.ExtractionResult<VariableReferenceExpression> decomposedPredicate = domainTranslator.fromPredicate(
                session.toConnectorSession(),
                deterministicPredicate,
                BASIC_COLUMN_EXTRACTOR);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(variableName -> node.getAssignments().get(variableName))
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint<ColumnHandle> constraint;
        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluatorForRowExpression evaluator = new LayoutConstraintEvaluatorForRowExpression(
                    metadata,
                    session,
                    node.getAssignments(),
                    logicalRowExpressions.combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(column -> assignments.getOrDefault(column, null)))));
            constraint = new Constraint<>(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint<>(newDomain);
        }
        if (constraint.getSummary().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        // Layouts will be returned in order of the connector's preference
        TableLayoutResult layout = metadata.getLayout(
                session,
                node.getTable(),
                constraint,
                Optional.of(node.getOutputVariables().stream()
                        .map(variable -> node.getAssignments().get(variable))
                        .collect(toImmutableSet())));

        if (layout.getLayout().getPredicate().isNone()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                layout.getLayout().getNewTableHandle(),
                node.getOutputVariables(),
                node.getAssignments(),
                layout.getLayout().getPredicate(),
                computeEnforced(newDomain, layout.getUnenforcedConstraint()));

        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        RowExpression resultingPredicate = logicalRowExpressions.combineConjuncts(
                domainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)),
                logicalRowExpressions.filterNonDeterministicConjuncts(predicate),
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_CONSTANT.equals(resultingPredicate)) {
            return new FilterNode(idAllocator.getNextId(), tableScan, resultingPredicate);
        }
        return tableScan;
    }

    private static String getColumnName(Session session, Metadata metadata, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
    }

    @Deprecated
    private static class LayoutConstraintEvaluatorForExpression
    {
        private final Map<VariableReferenceExpression, ColumnHandle> assignments;
        private final ExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluatorForExpression(Metadata metadata, SqlParser parser, Session session, TypeProvider types, Map<VariableReferenceExpression, ColumnHandle> assignments, Expression expression)
        {
            this.assignments = assignments;

            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, parser, types, expression, emptyList(), WarningCollector.NOOP);

            evaluator = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            arguments = VariablesExtractor.extractUnique(expression, types).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupVariableResolver inputs = new LookupVariableResolver(assignments, bindings, variable -> new Symbol(variable.getName()).toSymbolReference());

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null && !(optimized instanceof NullLiteral);
        }
    }

    private static class LayoutConstraintEvaluatorForRowExpression
    {
        private final Map<VariableReferenceExpression, ColumnHandle> assignments;
        private final RowExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluatorForRowExpression(Metadata metadata, Session session, Map<VariableReferenceExpression, ColumnHandle> assignments, RowExpression expression)
        {
            this.assignments = assignments;

            evaluator = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED);
            arguments = VariablesExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupVariableResolver inputs = new LookupVariableResolver(assignments, bindings, variable -> variable);

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null && (!(optimized instanceof ConstantExpression) || !((ConstantExpression) optimized).isNull());
        }
    }

    private static class LookupVariableResolver
            implements VariableResolver
    {
        private final Map<VariableReferenceExpression, ColumnHandle> assignments;
        private final Map<ColumnHandle, NullableValue> bindings;
        // Use Object type to let interpreters consume the result
        // TODO: use RowExpression once the Expression-to-RowExpression is done
        private final Function<VariableReferenceExpression, Object> missingBindingSupplier;

        public LookupVariableResolver(
                Map<VariableReferenceExpression, ColumnHandle> assignments,
                Map<ColumnHandle, NullableValue> bindings,
                Function<VariableReferenceExpression, Object> missingBindingSupplier)
        {
            this.assignments = requireNonNull(assignments, "assignments is null");
            this.bindings = ImmutableMap.copyOf(requireNonNull(bindings, "bindings is null"));
            this.missingBindingSupplier = requireNonNull(missingBindingSupplier, "missingBindingSupplier is null");
        }

        @Override
        public Object getValue(VariableReferenceExpression variable)
        {
            ColumnHandle column = assignments.get(variable);
            checkArgument(column != null, "Missing column assignment for %s", variable);

            if (!bindings.containsKey(column)) {
                return missingBindingSupplier.apply(variable);
            }

            return bindings.get(column).getValue();
        }
    }
}
