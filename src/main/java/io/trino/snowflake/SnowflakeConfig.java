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
package io.trino.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

public class SnowflakeConfig
{
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);
    private String warehouse = "";
    private String stageSchema = "stage";
    private int maxExportRetries = 3;
    private DataSize parquetMaxReadBlockSize = DataSize.ofBytes(16 * DataSize.Unit.MEGABYTE.inBytes());
    private DataSize maxSplitSize = DataSize.ofBytes(64 * DataSize.Unit.MEGABYTE.inBytes());
    private DataSize maxInitialSplitSize = DataSize.ofBytes(maxSplitSize.toBytes() / 2);
    private DataSize exportFileMaxSize = DataSize.ofBytes(DataSize.Unit.TERABYTE.inBytes());
    private boolean driverUseInformationSchema = true;
    private String database;
    //encrypted Private Key
    private String privateKey;
    private String passcode;
    private Boolean passcodeInPassword;
    private String privateKeyFile;
    private String privateKeyFilePassword;
    private String vaultUrl;
    private String vaultNamespace;
    private String vaultToken;
    private String vaultSecretPath;

    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("snowflake.warehouse")
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = this.warehouse;
        return this;
    }

    public String getStageSchema()
    {
        return stageSchema;
    }

    @Config("snowflake.stage-schema")
    public SnowflakeConfig setStageSchema(String stageSchema)
    {
        this.stageSchema = stageSchema;
        return this;
    }

    public int getMaxExportRetries()
    {
        return maxExportRetries;
    }

    @Config("snowflake.max.export.retries")
    public SnowflakeConfig setMaxExportRetries(int maxExportRetries)
    {
        this.maxExportRetries = maxExportRetries;
        return this;
    }

    public DataSize getParquetMaxReadBlockSize()
    {
        return parquetMaxReadBlockSize;
    }

    @Config("snowflake.parquet.max.block.size")
    public SnowflakeConfig setParquetMaxReadBlockSize(String parquetMaxReadBlockSize)
    {
        this.parquetMaxReadBlockSize = DataSize.valueOf(parquetMaxReadBlockSize);
        return this;
    }

    public DataSize getMaxSplitSize()
    {
        return maxSplitSize;
    }

    @Config("snowflake.max.split.size")
    public SnowflakeConfig setMaxSplitSize(String maxSplitSize)
    {
        this.maxSplitSize = DataSize.valueOf(maxSplitSize);
        return this;
    }

    public DataSize getMaxInitialSplitSize()
    {
        return maxInitialSplitSize;
    }

    @Config("snowflake.max.initial.split.size")
    public SnowflakeConfig setMaxInitialSplitSize(String maxInitialSplitSize)
    {
        this.maxInitialSplitSize = DataSize.valueOf(maxInitialSplitSize);
        return this;
    }

    public DataSize getExportFileMaxSize()
    {
        return exportFileMaxSize;
    }
    //snowflake.database
    //Name of schema in which stages are created for exporting data.
    // Number of export retries.3

    @Config("snowflake.export.file.size")
    public SnowflakeConfig setExportFileMaxSize(String exportFileMaxSize)
    {
        this.exportFileMaxSize = DataSize.valueOf(exportFileMaxSize);
        return this;
    }

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("snowflake.auto-reconnect")
    public SnowflakeConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("snowflake.max-reconnects")
    public SnowflakeConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("snowflake.connection-timeout")
    public SnowflakeConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }

    @Config("snowflake.jdbc.use-information-schema")
    @ConfigDescription("Value of useInformationSchema Snowflake JDBC driver connection property")
    public SnowflakeConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }

    public String getDatabase()
    {
        return database;
    }

    @Config("snowflake.database")
    public SnowflakeConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public String getPrivateKey()
    {
        return privateKey;
    }

    @Config("snowflake.privateKey")
    public SnowflakeConfig setPrivateKey(String privateKey)
    {
        this.privateKey = privateKey;
        return this;
    }

    public String getPasscode()
    {
        return passcode;
    }

    @Config("snowflake.passcode")
    public SnowflakeConfig setPasscode(String passcode)
    {
        this.passcode = passcode;
        return this;
    }

    public Boolean getPasscodeInPassword()
    {
        return passcodeInPassword;
    }

    @Config("snowflake.passcodeInPassword")
    public SnowflakeConfig setPasscodeInPassword(Boolean passcodeInPassword)
    {
        this.passcodeInPassword = passcodeInPassword;
        return this;
    }

    public String getPrivateKeyFile()
    {
        return privateKeyFile;
    }

    @Config("snowflake.private_key_file")
    public SnowflakeConfig setPrivateKeyFile(String privateKeyFile)
    {
        this.privateKeyFile = privateKeyFile;
        return this;
    }

    public String getPrivateKeyFilePassword()
    {
        return privateKeyFile;
    }

    @Config("snowflake.private_key_file_pwd")
    public SnowflakeConfig setPrivateKeyFilePassword(String privateKeyFilePassword)
    {
        this.privateKeyFilePassword = privateKeyFilePassword;
        return this;
    }

    public String getVaultUrl()
    {
        return vaultUrl;
    }

    @Config("vault.url")
    public SnowflakeConfig setVaultUrl(String vaultUrl)
    {
        this.vaultUrl = vaultUrl;
        return this;
    }

    public String getVaultNamespace()
    {
        return vaultNamespace;
    }

    @Config("vault.namespace")
    public SnowflakeConfig setVaultNamespace(String vaultNamespace)
    {
        this.vaultNamespace = vaultNamespace;
        return this;
    }

    public String getVaultToken()
    {
        return vaultToken;
    }

    @Config("vault.token")
    public SnowflakeConfig setVaultToken(String vaultToken)
    {
        this.vaultToken = vaultToken;
        return this;
    }

    public String getVaultSecretPath()
    {
        return vaultSecretPath;
    }

    @Config("vault.secretPath")
    public SnowflakeConfig setVaultSecretPath(String vaultSecretPath)
    {
        this.vaultSecretPath = vaultSecretPath;
        return this;
    }
}
