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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.snowflake.client.jdbc.SnowflakeDriver;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DecimalConfig;
import io.prestosql.plugin.jdbc.DecimalModule;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.TypeHandlingJdbcConfig;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SnowflakeClientModule
        implements Module
{
    private static void ensureCatalogIsEmpty(String connectionUrl)
    {
        try {
            Driver driver = new SnowflakeDriver();
            checkArgument(driver.acceptsURL(connectionUrl), "Invalid JDBC URL for Snowflake connector");
            //checkArgument(driver.database(urlProperties) == null, "Database (catalog) must not be specified in JDBC URL for MySQL connector");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, SnowflakeConfig snowflakeConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(snowflakeConfig.isDriverUseInformationSchema()));
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (snowflakeConfig.getWarehouse() != null) {
            connectionProperties.setProperty("warehouse", snowflakeConfig.getWarehouse());
        }
        if (snowflakeConfig.getDatabase() != null) {
            connectionProperties.setProperty("database", snowflakeConfig.getDatabase());
        }
        if (snowflakeConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(snowflakeConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(snowflakeConfig.getMaxReconnects()));
        }
        if (snowflakeConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(snowflakeConfig.getConnectionTimeout().toMillis()));
        }

        if (!snowflakeConfig.getPasscodeInPassword() && snowflakeConfig.getPasscode() != null) {
            connectionProperties.setProperty("passcode", snowflakeConfig.getPasscode());
        }
        else if (snowflakeConfig.getPasscodeInPassword()) {
            connectionProperties.setProperty("passcodeInPassword", snowflakeConfig.getPasscodeInPassword().toString());
        }

        if (snowflakeConfig.getVaultUrl() != null) {
            try {
                connectionProperties.put("privateKey", SnowflakeVault.getPrivateKeyFromVault(snowflakeConfig.getVaultUrl(), snowflakeConfig.getVaultNamespace(), snowflakeConfig.getVaultSecretPath(), snowflakeConfig.getVaultToken()));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        else {
            if (snowflakeConfig.getPrivateKey() != null && snowflakeConfig.getPrivateKeyFile() == null) {
                connectionProperties.setProperty("privateKey", snowflakeConfig.getPrivateKey());
            }

            if (snowflakeConfig.getPrivateKeyFile() != null) {
                connectionProperties.setProperty("private_key_file", snowflakeConfig.getPrivateKeyFile());
            }

            if (snowflakeConfig.getPrivateKeyFilePassword() != null) {
                connectionProperties.setProperty("private_key_file_pwd", snowflakeConfig.getPrivateKeyFilePassword());
            }
        }

        return new DriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SnowflakeClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(SnowflakeConfig.class);
        configBinder(binder).bindConfig(DecimalConfig.class);
        binder.install(new DecimalModule());
    }
}
