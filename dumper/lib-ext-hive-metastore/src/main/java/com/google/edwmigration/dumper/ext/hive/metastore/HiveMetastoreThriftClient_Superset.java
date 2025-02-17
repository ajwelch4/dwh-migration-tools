/*
 * Copyright 2022 Google LLC
 * Copyright 2013-2021 CompilerWorks
 *
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
package com.google.edwmigration.dumper.ext.hive.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the Thrift specification known to us to be a superset of the Thrift
 * specifications used in other Hive versions.
 *
 * This class is not thread-safe because it wraps an underlying
 * Thrift client which itself is not thread-safe.
 */
@NotThreadSafe
public class HiveMetastoreThriftClient_Superset extends HiveMetastoreThriftClient {

    @SuppressWarnings("UnusedVariable")
    private static final Logger LOG = LoggerFactory.getLogger(HiveMetastoreThriftClient_Superset.class);

    @Nonnull
    private final com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.ThriftHiveMetastore.Client client;

    // Deliberately not public
    /* pp */ HiveMetastoreThriftClient_Superset(@Nonnull String name, @Nonnull TProtocol protocol) {
        super(name);
        this.client = new com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.ThriftHiveMetastore.Client(protocol);
    }

    @Nonnull
    @Override
    public List<? extends String> getAllDatabaseNames() throws Exception {
        return client.get_all_databases();
    }

    @Nonnull
    @Override
    public List<? extends String> getAllTableNamesInDatabase(@Nonnull String databaseName) throws Exception {
        return client.get_all_tables(databaseName);
    }

    @Nonnull
    @Override
    public Table getTable(@Nonnull String databaseName, @Nonnull String tableName) throws Exception {
        com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Table table = client.get_table(databaseName, tableName);
        return new Table() {
            @CheckForNull
            @Override
            public String getDatabaseName() {
                return (table.isSetDbName() ? table.getDbName() : null);
            }

            @CheckForNull
            @Override
            public String getTableName() {
                return (table.isSetTableName() ? table.getTableName() : null);
            }

            @CheckForNull
            @Override
            public String getTableType() {
                return (table.isSetTableType() ? table.getTableType() : null);
            }

            @CheckForNull
            @Override
            public Integer getCreateTime() {
                return (table.isSetCreateTime() ? table.getCreateTime() : null);
            }

            @CheckForNull
            @Override
            public Integer getLastAccessTime() {
                return (table.isSetLastAccessTime() ? table.getLastAccessTime() : null);
            }

            @CheckForNull
            @Override
            public String getOwner() {
                return (table.isSetOwner() ? table.getOwner() : null);
            }

            @CheckForNull
            @Override
            public String getOriginalViewText() {
                return (table.isSetViewOriginalText() ? table.getViewOriginalText() : null);
            }

            @CheckForNull
            @Override
            public String getExpandedViewText() {
                return (table.isSetViewExpandedText() ? table.getViewExpandedText() : null);
            }

            @CheckForNull
            @Override
            public String getLocation() {
                return (table.isSetSd() && table.getSd().isSetLocation() ? table.getSd().getLocation() : null);
            }

            @Nonnull
            @Override
            public List<? extends Field> getFields() throws Exception {
                List<Field> out = new ArrayList<>();
                for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema field : client.get_fields(databaseName, tableName)) {
                    out.add(new Field() {
                        @CheckForNull
                        @Override
                        public String getFieldName() {
                            return (field.isSetName() ? field.getName() : null);
                        }

                        @CheckForNull
                        @Override
                        public String getType() {
                            return (field.isSetType() ? field.getType() : null);
                        }

                        @CheckForNull
                        @Override
                        public String getComment() {
                            return (field.isSetComment() ? field.getComment() : null);
                        }
                    });
                }
                return out;
            }

            @Nonnull
            @Override
            public List<? extends PartitionKey> getPartitionKeys() {
                List<PartitionKey> out = new ArrayList<>();
                for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.FieldSchema partitionKey : table.getPartitionKeys()) {
                    out.add(new PartitionKey() {
                        @CheckForNull
                        @Override
                        public String getPartitionKeyName() {
                            return (partitionKey.isSetName() ? partitionKey.getName() : null);
                        }

                        @CheckForNull
                        @Override
                        public String getType() {
                            return (partitionKey.isSetType() ? partitionKey.getType() : null);
                        }

                        @CheckForNull
                        @Override
                        public String getComment() {
                            return (partitionKey.isSetComment() ? partitionKey.getComment() : null);
                        }
                    });
                }
                return out;
            }

            @Nonnull
            @Override
            public List<? extends Partition> getPartitions() throws Exception {
                List<Partition> out = new ArrayList<>();
                for (String partitionName : client.get_partition_names(databaseName, tableName, Short.MAX_VALUE)) {
                    com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Partition partition = client.get_partition_by_name(databaseName, tableName, partitionName);
                    out.add(new Partition() {
                        @Nonnull
                        @Override
                        public String getPartitionName() {
                            return partitionName;
                        }

                        @CheckForNull
                        @Override
                        public String getLocation() {
                            return (partition.isSetSd() && partition.getSd().isSetLocation() ? partition.getSd().getLocation() : null);
                        }
                    });
                }
                return out;
            }
        };
    }

    @Nonnull
    @Override
    public List<? extends Function> getFunctions() throws Exception {
        com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.GetAllFunctionsResponse allFunctions = client.get_all_functions();
        List<Function> out = new ArrayList<>();
        for (com.google.edwmigration.dumper.ext.hive.metastore.thrift.api.superset.Function function : allFunctions.getFunctions()) {
            out.add(new Function() {
                @CheckForNull
                @Override
                public String getDatabaseName() {
                    return (function.isSetDbName() ? function.getDbName() : null);
                }

                @CheckForNull
                @Override
                public String getFunctionName() {
                    return (function.isSetFunctionName() ? function.getFunctionName() : null);
                }

                @CheckForNull
                @Override
                public String getType() {
                    return (function.isSetFunctionType() ? function.getFunctionType().toString() : null);
                }

                @CheckForNull
                @Override
                public String getClassName() {
                    return (function.isSetClassName() ? function.getClassName() : null);
                }
            });
        }
        return out;
    }

    @Override
    public void close() throws IOException {
        try {
            client.shutdown();
        } catch (TException e) {
            throw new IOException("Unable to shutdown Thrift client.", e);
        }
    }
}
