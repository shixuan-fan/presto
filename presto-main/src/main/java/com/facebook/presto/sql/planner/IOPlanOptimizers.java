package com.facebook.presto.sql.planner;

import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.rule.PickTableLayout;
import com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PredicatePushDown;
import com.facebook.presto.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.WindowFilterPushDown;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

public class IOPlanOptimizers
{
    private final List<PlanOptimizer> optimizers;

    @Inject
    public IOPlanOptimizers(
            Metadata metadata,
            SqlParser sqlParser,
            StatsCalculator statsCalculator,
            CostCalculator estimatedExchangesCostCalculator)
    {
        ImmutableList.Builder<PlanOptimizer> builder = ImmutableList.builder();

        RuleStatsRecorder ruleStats = new RuleStatsRecorder();
        IterativeOptimizer simplifyOptimizer = new IterativeOptimizer(
                ruleStats,
                statsCalculator,
                estimatedExchangesCostCalculator,
                new SimplifyExpressions(metadata, sqlParser).rules());

        OptimizerStatsRecorder optimizerStats = new OptimizerStatsRecorder();
        PlanOptimizer predicatePushDown = new StatsRecordingPlanOptimizer(optimizerStats, new PredicatePushDown(metadata, sqlParser));

        builder.add(
                predicatePushDown,
                simplifyOptimizer, // Re-run the SimplifyExpressions to simplify any recomposed expressions from other optimizations
                new WindowFilterPushDown(metadata), // This must run after PredicatePushDown and LimitPushDown so that it squashes any successive filter nodes and limits
                predicatePushDown,
                simplifyOptimizer, // Should be always run after PredicatePushDown
                new IterativeOptimizer(
                        ruleStats,
                        statsCalculator,
                        estimatedExchangesCostCalculator,
                        new PickTableLayout(metadata, sqlParser).rules()),
                predicatePushDown,
                simplifyOptimizer);

        this.optimizers = builder.build();
    }

    public List<PlanOptimizer> get()
    {
        return optimizers;
    }
}
