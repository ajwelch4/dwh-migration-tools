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
package com.google.pojo;

import static com.google.csv.CsvUtil.getIntNotNull;
import static com.google.csv.CsvUtil.getStringNotNull;
import static java.lang.System.lineSeparator;

import com.google.auto.value.AutoValue;
import com.google.jdbc.JdbcUtil;
import java.sql.ResultSet;
import java.sql.SQLException;

/** POJO class for serialization data from DB and CSV files. */
@AutoValue
public abstract class SvvColumnsRow {

  public static SvvColumnsRow create(
      String tableCatalog,
      String tableSchema,
      String tableName,
      String columnName,
      int ordinalPosition,
      String columnDefault,
      String isNullable,
      String dataType,
      int characterMaximumLength,
      int numericPrecision,
      int numericPrecisionRadix,
      int numericScale,
      int datetimePrecision,
      String intervalType,
      String intervalPrecision,
      String characterSetCatalog,
      String characterSetSchema,
      String characterSetName,
      String collationCatalog,
      String collationSchema,
      String collationName,
      String domainName,
      String remarks) {
    return new AutoValue_SvvColumnsRow(
        tableCatalog,
        tableSchema,
        tableName,
        columnName,
        ordinalPosition,
        columnDefault,
        isNullable,
        dataType,
        characterMaximumLength,
        numericPrecision,
        numericPrecisionRadix,
        numericScale,
        datetimePrecision,
        intervalType,
        intervalPrecision,
        characterSetCatalog,
        characterSetSchema,
        characterSetName,
        collationCatalog,
        collationSchema,
        collationName,
        domainName,
        remarks);
  }

  public static SvvColumnsRow create(ResultSet rs) throws SQLException {
    return SvvColumnsRow.create(
        JdbcUtil.getStringNotNull(rs, "table_catalog"),
        JdbcUtil.getStringNotNull(rs, "table_schema"),
        JdbcUtil.getStringNotNull(rs, "table_name"),
        JdbcUtil.getStringNotNull(rs, "column_name"),
        JdbcUtil.getIntNotNull(rs, "ordinal_position"),
        JdbcUtil.getStringNotNull(rs, "column_default"),
        JdbcUtil.getStringNotNull(rs, "is_nullable"),
        JdbcUtil.getStringNotNull(rs, "data_type"),
        JdbcUtil.getIntNotNull(rs, "character_maximum_length"),
        JdbcUtil.getIntNotNull(rs, "numeric_precision"),
        JdbcUtil.getIntNotNull(rs, "numeric_precision_radix"),
        JdbcUtil.getIntNotNull(rs, "numeric_scale"),
        JdbcUtil.getIntNotNull(rs, "datetime_precision"),
        JdbcUtil.getStringNotNull(rs, "interval_type"),
        JdbcUtil.getStringNotNull(rs, "interval_precision"),
        JdbcUtil.getStringNotNull(rs, "character_set_catalog"),
        JdbcUtil.getStringNotNull(rs, "character_set_schema"),
        JdbcUtil.getStringNotNull(rs, "character_set_name"),
        JdbcUtil.getStringNotNull(rs, "collation_catalog"),
        JdbcUtil.getStringNotNull(rs, "collation_schema"),
        JdbcUtil.getStringNotNull(rs, "collation_name"),
        JdbcUtil.getStringNotNull(rs, "domain_name"),
        JdbcUtil.getStringNotNull(rs, "remarks"));
  }

  public static SvvColumnsRow create(String[] csvLine) {
    return new AutoValue_SvvColumnsRow(
        getStringNotNull(csvLine[0]),
        getStringNotNull(csvLine[1]),
        getStringNotNull(csvLine[2]),
        getStringNotNull(csvLine[3]),
        getIntNotNull(csvLine[4]),
        getStringNotNull(csvLine[5]),
        getStringNotNull(csvLine[6]),
        getStringNotNull(csvLine[7]),
        getIntNotNull(csvLine[8]),
        getIntNotNull(csvLine[9]),
        getIntNotNull(csvLine[10]),
        getIntNotNull(csvLine[11]),
        getIntNotNull(csvLine[12]),
        getStringNotNull(csvLine[13]),
        getStringNotNull(csvLine[14]),
        getStringNotNull(csvLine[15]),
        getStringNotNull(csvLine[16]),
        getStringNotNull(csvLine[17]),
        getStringNotNull(csvLine[18]),
        getStringNotNull(csvLine[19]),
        getStringNotNull(csvLine[20]),
        getStringNotNull(csvLine[21]),
        getStringNotNull(csvLine[22]));
  }

  public abstract String tableCatalog();

  public abstract String tableSchema();

  public abstract String tableName();

  public abstract String columnName();

  public abstract int ordinalPosition();

  public abstract String columnDefault();

  public abstract String isNullable();

  public abstract String dataType();

  public abstract int characterMaximumLength();

  public abstract int numericPrecision();

  public abstract int numericPrecisionRadix();

  public abstract int numericScale();

  public abstract int datetimePrecision();

  public abstract String intervalType();

  public abstract String intervalPrecision();

  public abstract String characterSetCatalog();

  public abstract String characterSetSchema();

  public abstract String characterSetName();

  public abstract String collationCatalog();

  public abstract String collationSchema();

  public abstract String collationName();

  public abstract String domainName();

  public abstract String remarks();

  @Override
  public String toString() {
    return "tableCatalog="
        + tableCatalog()
        + ", tableSchema="
        + tableSchema()
        + ", tableName="
        + tableName()
        + ", columnName="
        + columnName()
        + ", ordinalPosition="
        + ordinalPosition()
        + ", columnDefault="
        + columnDefault()
        + ", isNullable="
        + isNullable()
        + ", dataType="
        + dataType()
        + ", characterMaximumLength="
        + characterMaximumLength()
        + ", numericPrecision="
        + numericPrecision()
        + ", numericPrecisionRadix="
        + numericPrecisionRadix()
        + ", numericScale="
        + numericScale()
        + ", datetimePrecision="
        + datetimePrecision()
        + ", intervalType="
        + intervalType()
        + ", intervalPrecision="
        + intervalPrecision()
        + ", characterSetCatalog="
        + characterSetCatalog()
        + ", characterSetSchema="
        + characterSetSchema()
        + ", characterSetName="
        + characterSetName()
        + ", collationCatalog="
        + collationCatalog()
        + ", collationSchema="
        + collationSchema()
        + ", collationName="
        + collationName()
        + ", domainName="
        + domainName()
        + ", remarks="
        + remarks()
        + lineSeparator();
  }
}
