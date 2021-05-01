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

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.LogicalResponse;

import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;

public class SnowflakeVault
{
    private SnowflakeVault()
    {
    }

    public static final String SNOWSQL_PRIVATE_KEY_PASSPHRASE = "SNOWSQL_PRIVATE_KEY_PASSPHRASE";
    public static final String PRIVATE_KEY = "private_key";

    public static PrivateKey getPrivateKeyFromVault(String url, String namespace, String secretPath, String token)
            throws VaultException, InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, IOException
    {
        final VaultConfig config = new VaultConfig()
                .address(url)
                .token(token)
                .nameSpace(namespace)
                .build();
        final Vault vault = new Vault(config, 1);
        LogicalResponse response = vault.logical().read(secretPath);
        System.out.println(String.format("Status: %d", response.getRestResponse().getStatus()));
        Map<String, String> data = response.getData();
        String passphrase = data.get(SNOWSQL_PRIVATE_KEY_PASSPHRASE);
        String privateKey = data.get(PRIVATE_KEY);
        return getPrivateKey(passphrase, privateKey);
    }

    public static PrivateKey getPrivateKey(String passphrase, String encrypted)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException
    {
        encrypted = encrypted
                .replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "")
                .replace("-----END ENCRYPTED PRIVATE KEY-----", "");
        EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
        PBEKeySpec keySpec = new PBEKeySpec(passphrase.toCharArray());
        SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
        PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(encodedKeySpec);
    }
}
