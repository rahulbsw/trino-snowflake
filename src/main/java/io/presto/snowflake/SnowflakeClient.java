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
package io.presto.snowflake;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.PredicatePushdownController;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.prestosql.plugin.jdbc.PredicatePushdownController.PUSHDOWN_AND_KEEP;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class SnowflakeClient
        extends BaseJdbcClient
{
    private final Type jsonType;
    private final AggregateFunctionRewriter aggregateFunctionRewriter;

    @Inject
    public SnowflakeClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, "`", connectionFactory);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(SnowflakeClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        // .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        // Abort connection before closing. Without this, the Snowflake driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> unsignedMapping = getUnsignedMapping(typeHandle);
        if (unsignedMapping.isPresent()) {
            return unsignedMapping;
        }

        if (jdbcTypeName.equalsIgnoreCase("json")) {
            return Optional.of(jsonColumnMapping());
        }

        switch (typeHandle.getJdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.getRequiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize()));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                int varcharLength = typeHandle.getRequiredColumnSize();
                VarcharType varcharType = (varcharLength <= VarcharType.MAX_LENGTH) ? createVarcharType(varcharLength) : createUnboundedVarcharType();
                // Remote database can be case insensitive.
                PredicatePushdownController predicatePushdownController = PUSHDOWN_AND_KEEP;
                return Optional.of(ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), predicatePushdownController));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMapping());
        }

        // TODO add explicit mappings
        return legacyToPrestoType(session, connection, typeHandle);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_TZ_MILLIS.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            return WriteMapping.longMapping("datetime", timestampWriteFunction(TIMESTAMP_MILLIS));
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            else if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            }
            else if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        return legacyToWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static Optional<ColumnMapping> getUnsignedMapping(JdbcTypeHandle typeHandle)
    {
        if (!typeHandle.getJdbcTypeName().isPresent()) {
            return Optional.empty();
        }

        String typeName = typeHandle.getJdbcTypeName().get();
        if (typeName.equalsIgnoreCase("tinyint unsigned")) {
            return Optional.of(smallintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("smallint unsigned")) {
            return Optional.of(integerColumnMapping());
        }
        if (typeName.equalsIgnoreCase("int unsigned")) {
            return Optional.of(bigintColumnMapping());
        }
        if (typeName.equalsIgnoreCase("bigint unsigned")) {
            return Optional.of(decimalColumnMapping(createDecimalType(20)));
        }

        return Optional.empty();
    }
}
