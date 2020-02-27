package test;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.ImmutableBitSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;




public class UpdateTest {
    
    private static final List<RelTraitDef> TRAITS = Collections
            .unmodifiableList(java.util.Arrays.asList(ConventionTraitDef.INSTANCE,
                    RelCollationTraitDef.INSTANCE));
    
    private static final SqlParser.Config SQL_PARSER_CONFIG =
            SqlParser.configBuilder(SqlParser.Config.DEFAULT)
                    .setCaseSensitive(false)
                    .setConformance(SqlConformanceEnum.MYSQL_5)
                    .setQuoting(Quoting.BACK_TICK)
                    .build();
    @Test
    public void hello() throws Exception {            
        
        // CREATE TABLE ss.mytable (
        // field0 VARCHAR PRIMARY KEY
        // field1 DOUBLE,
        // field2 BINARY,
        // field3 DATE,
        // field4 SMALLINT
        // )
        String schemaName = "ss";
        String query
                = "UPDATE "+schemaName+".mytable"
                + "  set field3=?," // DATE
                + "      field4=?," // SMALLINT
                + "      field1=?," // DOUBLE
                + "      field2=?"  // BINARY
                + "  where field0 = ?"; 
        SchemaPlus subSchema = buidlSchema(schemaName);
     FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SQL_PARSER_CONFIG)
                .defaultSchema(subSchema)
                .traitDefs(TRAITS)
                // define the rules you want to apply

                .programs(Programs.ofRules(Programs.RULE_SET))
                .build();
        Planner planner = Frameworks.getPlanner(config);
        System.out.println("query:"+query);
        
        SqlNode n = planner.parse(query);
        n = planner.validate(n);
        RelNode logicalPlan = planner.rel(n).project();
        String dumpPlan = RelOptUtil.dumpPlan("-- Best  Plan", logicalPlan, SqlExplainFormat.TEXT,
                SqlExplainLevel.ALL_ATTRIBUTES);
        System.out.println(dumpPlan);
        LogicalTableModify main = (LogicalTableModify) logicalPlan;
        assertEquals(Arrays.asList("field3","field4","field1","field2"), main.getUpdateColumnList());
        List<RexNode> sourceExpressionList = main.getSourceExpressionList();
        
        // failed:  expected: <DATE> but was: <DOUBLE>
        RexDynamicParam field3value = (RexDynamicParam) sourceExpressionList.get(0);
        assertEquals("DATE", field3value.getType().getSqlTypeName());
        RexDynamicParam field4value = (RexDynamicParam) sourceExpressionList.get(1);
        assertEquals("SMALLINT", field4value.getType().getSqlTypeName());
        RexDynamicParam field1value = (RexDynamicParam) sourceExpressionList.get(2);
        assertEquals("DOUBLE", field1value.getType().getSqlTypeName());
        RexDynamicParam field2value = (RexDynamicParam) sourceExpressionList.get(3);
        assertEquals("BINARY", field2value.getType().getSqlTypeName());
}

    private SchemaPlus buidlSchema(String schemaName) {
        final SchemaPlus _rootSchema = Frameworks.createRootSchema(true);
        SchemaPlus schema = _rootSchema.add(schemaName, new AbstractSchema());        
        schema.add("mytable", new TableImpl());
        return _rootSchema;
    }
    
     private static final class TableImpl extends AbstractTable
            implements ModifiableTable, ScannableTable, ProjectableFilterableTable {

        
        final ImmutableList<ImmutableBitSet> keys;

        private TableImpl() {
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
            builder.set(0); // first field is the PK
            
            keys = ImmutableList.of(builder.build());
        }


        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            builder.add("field0", typeFactory.createSqlType(SqlTypeName.VARCHAR));
            
            builder.add("field1", typeFactory.createSqlType(SqlTypeName.DOUBLE));
            builder.add("field2", typeFactory.createSqlType(SqlTypeName.BINARY));
            builder.add("field3", typeFactory.createSqlType(SqlTypeName.DATE));
            builder.add("field4", typeFactory.createSqlType(SqlTypeName.SMALLINT));
            
            
            return builder.build();
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.of(1000,
                    keys);
        }

        @Override
        public Collection getModifiableCollection() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
            return LogicalTableModify.create(table, catalogReader, child, operation,
                    updateColumnList, sourceExpressionList, flattened);
        }

        @Override
        public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Type getElementType() {
            return Object.class;
        }

        @Override
        public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
            return null;
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
     }
}
