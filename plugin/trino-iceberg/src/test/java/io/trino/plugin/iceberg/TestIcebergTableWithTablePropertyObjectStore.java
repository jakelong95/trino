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
package io.trino.plugin.iceberg;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergTableWithTablePropertyObjectStore
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystem fileSystem;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(Map.of("iceberg.object-store.enabled", "false"))
                .build();

        metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        fileSystem = getFileSystemFactory(queryRunner).create(SESSION);

        return queryRunner;
    }

    @Test
    void testCreateWithTablePropertyAndDrop()
            throws Exception
    {
        assertQuerySucceeds("CREATE TABLE test_create_and_drop WITH (object_store_enabled = TRUE, data_location = 'local:///table-location/xyz') AS SELECT 1 AS val");
        Table table = metastore.getTable("tpch", "test_create_and_drop").orElseThrow();
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isTrue();

        String filePath = (String) computeScalar("SELECT file_path FROM \"test_create_and_drop$files\"");
        Location dataFileLocation = Location.of(filePath);
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isTrue();
        assertThat(filePath).matches("local:///table-location/xyz/.{6}/tpch/test_create_and_drop.*");

        assertQuerySucceeds("DROP TABLE test_create_and_drop");
        assertThat(metastore.getTable("tpch", "test_create_and_drop")).isEmpty();
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isFalse();
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isFalse();
    }

    @Test
    void testCreateTableThenEnableObjectStore()
            throws Exception
    {
        assertQuerySucceeds("CREATE TABLE test_create_then_enable_object_store AS SELECT 1 AS val");
        Table table = metastore.getTable("tpch", "test_create_then_enable_object_store").orElseThrow();
        assertThat(table.getTableType()).isEqualTo(EXTERNAL_TABLE.name());

        Location tableLocation = Location.of(table.getStorage().getLocation());
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isTrue();

        String filePath = (String) computeScalar("SELECT file_path FROM \"test_create_then_enable_object_store$files\"");
        Location dataFileLocation = Location.of(filePath);
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isTrue();
        assertThat(filePath).startsWith("local:///tpch/test_create_then_enable_object_store");

        assertUpdate("ALTER TABLE test_create_then_enable_object_store SET PROPERTIES object_store_enabled = TRUE, data_location = 'local:///table-location-2/xyz'");
        assertUpdate("INSERT INTO test_create_then_enable_object_store VALUES 2", 1);

        Set<Object> filePaths = computeActual("SELECT file_path FROM \"test_create_then_enable_object_store$files\"").getOnlyColumnAsSet();
        assertThat(filePaths.stream().map(Object::toString)).anySatisfy(newPath -> {
            Location newDataFileLocation = Location.of(newPath);

            assertThat(fileSystem.newInputFile(newDataFileLocation).exists()).isTrue();
            assertThat(newPath).startsWith("local:///tpch/test_create_then_enable_object_store");
        });

        assertQuerySucceeds("DROP TABLE test_create_then_enable_object_store");
        assertThat(metastore.getTable("tpch", "test_create_then_enable_object_store")).isEmpty();
        assertThat(fileSystem.newInputFile(dataFileLocation).exists()).isFalse();
        assertThat(fileSystem.newInputFile(tableLocation).exists()).isFalse();
    }
}
