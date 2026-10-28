package dev.dimster.flink.runner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Entry point of the runner jar: the Flink job of one Dimster test point.
 *
 * <p>The coordinator uploads this jar to the Flink session cluster and runs it
 * with a {@link FlinkJobSpec}. For each (input, output) topic pair it creates
 * a {@code source} table in its own database {@code p<i>}, so the workload's
 * SELECT resolves {@code source} to that pair's input. It then derives the
 * sink from the query's resolved schema ({@link DdlBuilder}) and stamps each
 * output record with its emit time ({@link EmitTimeHeaders}). All pairs go
 * into one statement set, so the test point is a single job and its pipelines
 * share slots.
 */
public final class DimsterSqlJob {

    private DimsterSqlJob() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected one argument, the base64-encoded job spec");
        }
        FlinkJobSpec spec = FlinkJobSpec.fromBase64(args[0]);
        TableEnvironment tEnv = TableEnvironment.create(settings(spec));
        // Submits the job. In a session cluster the REST jar-run call returns
        // as soon as the job is submitted.
        buildStatementSet(tEnv, spec).execute();
    }

    /** Job-level configuration: name, parallelism, checkpointing, then the workload's overrides. */
    static EnvironmentSettings settings(FlinkJobSpec spec) {
        Map<String, String> conf = new HashMap<>();
        conf.put("pipeline.name", spec.jobName());
        conf.put("parallelism.default", Integer.toString(spec.parallelism()));
        if (spec.checkpointIntervalMs() > 0) {
            conf.put("execution.checkpointing.interval", spec.checkpointIntervalMs() + " ms");
        }
        conf.putAll(spec.config());
        return EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(Configuration.fromMap(conf))
                .build();
    }

    /** Registers every pipeline's tables and adds its INSERT to one statement set. */
    static StatementSet buildStatementSet(TableEnvironment tEnv, FlinkJobSpec spec) {
        if (spec.pipelines().isEmpty()) {
            throw new IllegalArgumentException("The job spec has no pipelines");
        }
        String catalog = tEnv.getCurrentCatalog();
        StatementSet statements = tEnv.createStatementSet();
        for (int i = 0; i < spec.pipelines().size(); i++) {
            String database = "p" + i;
            tEnv.executeSql("CREATE DATABASE " + DdlBuilder.quote(database));
            tEnv.useDatabase(database);
            tEnv.executeSql(DdlBuilder.sourceDdl(spec, spec.pipelines().get(i)));

            Table query = tEnv.sqlQuery(spec.sql());
            DdlBuilder.SinkLayout layout = DdlBuilder.sinkLayout(
                    columns(query.getResolvedSchema()),
                    columns(tEnv.from("source").getResolvedSchema()));
            tEnv.executeSql(DdlBuilder.sinkDdl(spec, i, layout));

            // Sink columns are matched by position: the query's columns in
            // layout order, then the headers column that sinkDdl puts last.
            List<Expression> sinkColumns = new ArrayList<>(layout.columnNames().stream()
                    .map(Expressions::$)
                    .toList());
            sinkColumns.add(Expressions.call(EmitTimeHeaders.class).as(DdlBuilder.HEADERS));
            Table ordered = query.select(sinkColumns.toArray(Expression[]::new));
            statements.add(ordered.insertInto(
                    DdlBuilder.quote(catalog) + "." + DdlBuilder.quote(database) + "." + DdlBuilder.quote("sink")));
        }
        return statements;
    }

    private static List<DdlBuilder.SchemaColumn> columns(ResolvedSchema schema) {
        return schema.getColumns().stream()
                .map(c -> new DdlBuilder.SchemaColumn(c.getName(),
                        c.getDataType().getLogicalType().asSerializableString()))
                .toList();
    }
}
