/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.source.adhocquery;

import com.google.common.collect.ImmutableSet;

public class PushDownConverterKeyWords {

    private PushDownConverterKeyWords() {
    }

    public static final ImmutableSet<String> CALCITE = ImmutableSet.of("A", "ABS", "ABSOLUTE", "ACTION", "ADA", "ADD",
            "ADMIN", "AFTER", "ALL", "ALLOCATE", "ALLOW", "ALTER", "ALWAYS", "AND", "ANY", "APPLY", "ARE", "ARRAY",
            "ARRAY_MAX_CARDINALITY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATOMIC",
            "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "AVG", "BEFORE", "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION",
            "BERNOULLI", "BETWEEN", "BIGINT", "BINARY", "BIT", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BY", "C", "CALL",
            "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CATALOG_NAME", "CEIL",
            "CEILING", "CENTURY", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH",
            "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHAR_LENGTH", "CHECK", "CLASSIFIER",
            "CLASS_ORIGIN", "CLOB", "CLOSE", "COALESCE", "COBOL", "COLLATE", "COLLATION", "COLLATION_CATALOG",
            "COLLATION_NAME", "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMN_NAME", "COMMAND_FUNCTION",
            "COMMAND_FUNCTION_CODE", "COMMIT", "COMMITTED", "CONDITION", "CONDITION_NUMBER", "CONNECT", "CONNECTION",
            "CONNECTION_NAME", "CONSTRAINT", "CONSTRAINTS", "CONSTRAINT_CATALOG", "CONSTRAINT_NAME",
            "CONSTRAINT_SCHEMA", "CONSTRUCTOR", "CONTAINS", "CONTINUE", "CONVERT", "CORR", "CORRESPONDING", "COUNT",
            "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG",
            "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_ROW",
            "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER",
            "CURSOR", "CURSOR_NAME", "CYCLE", "DATA", "DATABASE", "DATE", "DATETIME_INTERVAL_CODE",
            "DATETIME_INTERVAL_PRECISION", "DAY", "DEALLOCATE", "DEC", "DECADE", "DECIMAL", "DECLARE", "DEFAULT",
            "DEFAULTS", "DEFERRABLE", "DEFERRED", "DEFINE", "DEFINED", "DEFINER", "DEGREE", "DELETE", "DENSE_RANK",
            "DEPTH", "DEREF", "DERIVED", "DESC", "DESCRIBE", "DESCRIPTION", "DESCRIPTOR", "DETERMINISTIC",
            "DIAGNOSTICS", "DISALLOW", "DISCONNECT", "DISPATCH", "DISTINCT", "DOMAIN", "DOUBLE", "DOW", "DOY", "DROP",
            "DYNAMIC", "DYNAMIC_FUNCTION", "DYNAMIC_FUNCTION_CODE", "EACH", "ELEMENT", "ELSE", "EMPTY", "END",
            "END-EXEC", "END_FRAME", "END_PARTITION", "EPOCH", "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION",
            "EXCLUDE", "EXCLUDING", "EXEC", "EXECUTE", "EXISTS", "EXP", "EXPLAIN", "EXTEND", "EXTERNAL", "EXTRACT",
            "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FLOAT", "FLOOR", "FOLLOWING", "FOR",
            "FOREIGN", "FORTRAN", "FOUND", "FRAC_SECOND", "FRAME_ROW", "FREE", "FROM", "FULL", "FUNCTION", "FUSION",
            "G", "GENERAL", "GENERATED", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GROUP", "GROUPING",
            "GROUPS", "HAVING", "HIERARCHY", "HOLD", "HOUR", "IDENTITY", "IMMEDIATE", "IMMEDIATELY", "IMPLEMENTATION",
            "IMPORT", "IN", "INCLUDING", "INCREMENT", "INDICATOR", "INITIAL", "INITIALLY", "INNER", "INOUT", "INPUT",
            "INSENSITIVE", "INSERT", "INSTANCE", "INSTANTIABLE", "INT", "INTEGER", "INTERSECT", "INTERSECTION",
            "INTERVAL", "INTO", "INVOKER", "IS", "ISOLATION", "JAVA", "JOIN", "JSON", "K", "KEY", "KEY_MEMBER",
            "KEY_TYPE", "LABEL", "LAG", "LANGUAGE", "LARGE", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT",
            "LENGTH", "LEVEL", "LIBRARY", "LIKE", "LIKE_REGEX", "LIMIT", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP",
            "LOCATOR", "LOWER", "M", "MAP", "MATCH", "MATCHED", "MATCHES", "MATCH_NUMBER", "MATCH_RECOGNIZE", "MAX",
            "MAXVALUE", "MEASURES", "MEMBER", "MERGE", "MESSAGE_LENGTH", "MESSAGE_OCTET_LENGTH", "MESSAGE_TEXT",
            "METHOD", "MICROSECOND", "MILLENNIUM", "MIN", "MINUS", "MINUTE", "MINVALUE", "MOD", "MODIFIES", "MODULE",
            "MONTH", "MORE", "MULTISET", "MUMPS", "NAME", "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NESTING",
            "NEW", "NEXT", "NO", "NONE", "NORMALIZE", "NORMALIZED", "NOT", "NTH_VALUE", "NTILE", "NULL", "NULLABLE",
            "NULLIF", "NULLS", "NUMBER", "NUMERIC", "OBJECT", "OCCURRENCES_REGEX", "OCTETS", "OCTET_LENGTH", "OF",
            "OFFSET", "OLD", "OMIT", "ON", "ONE", "ONLY", "OPEN", "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING",
            "ORDINALITY", "OTHERS", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING", "PAD",
            "PARAMETER", "PARAMETER_MODE", "PARAMETER_NAME", "PARAMETER_ORDINAL_POSITION", "PARAMETER_SPECIFIC_CATALOG",
            "PARAMETER_SPECIFIC_NAME", "PARAMETER_SPECIFIC_SCHEMA", "PARTIAL", "PARTITION", "PASCAL", "PASSTHROUGH",
            "PAST", "PATH", "PATTERN", "PER", "PERCENT", "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD",
            "PERMUTE", "PLACING", "PLAN", "PLI", "PORTION", "POSITION", "POSITION_REGEX", "POWER", "PRECEDES",
            "PRECEDING", "PRECISION", "PREPARE", "PRESERVE", "PREV", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE",
            "PUBLIC", "QUARTER", "RANGE", "RANK", "READ", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES",
            "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE",
            "REGR_SXX", "REGR_SXY", "REGR_SYY", "RELATIVE", "RELEASE", "REPEATABLE", "REPLACE", "RESET", "RESTART",
            "RESTRICT", "RESULT", "RETURN", "RETURNED_CARDINALITY", "RETURNED_LENGTH", "RETURNED_OCTET_LENGTH",
            "RETURNED_SQLSTATE", "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE",
            "ROUTINE_CATALOG", "ROUTINE_NAME", "ROUTINE_SCHEMA", "ROW", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RUNNING",
            "SAVEPOINT", "SCALE", "SCHEMA", "SCHEMA_NAME", "SCOPE", "SCOPE_CATALOGS", "SCOPE_NAME", "SCOPE_SCHEMA",
            "SCROLL", "SEARCH", "SECOND", "SECTION", "SECURITY", "SEEK", "SELECT", "SELF", "SENSITIVE", "SEQUENCE",
            "SERIALIZABLE", "SERVER", "SERVER_NAME", "SESSION", "SESSION_USER", "SET", "SETS", "SHOW", "SIMILAR",
            "SIMPLE", "SIZE", "SKIP", "SMALLINT", "SOME", "SOURCE", "SPACE", "SPECIFIC", "SPECIFICTYPE",
            "SPECIFIC_NAME", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIGINT", "SQL_BINARY", "SQL_BIT",
            "SQL_BLOB", "SQL_BOOLEAN", "SQL_CHAR", "SQL_CLOB", "SQL_DATE", "SQL_DECIMAL", "SQL_DOUBLE", "SQL_FLOAT",
            "SQL_INTEGER", "SQL_INTERVAL_DAY", "SQL_INTERVAL_DAY_TO_HOUR", "SQL_INTERVAL_DAY_TO_MINUTE",
            "SQL_INTERVAL_DAY_TO_SECOND", "SQL_INTERVAL_HOUR", "SQL_INTERVAL_HOUR_TO_MINUTE",
            "SQL_INTERVAL_HOUR_TO_SECOND", "SQL_INTERVAL_MINUTE", "SQL_INTERVAL_MINUTE_TO_SECOND", "SQL_INTERVAL_MONTH",
            "SQL_INTERVAL_SECOND", "SQL_INTERVAL_YEAR", "SQL_INTERVAL_YEAR_TO_MONTH", "SQL_LONGVARBINARY",
            "SQL_LONGVARCHAR", "SQL_LONGVARNCHAR", "SQL_NCHAR", "SQL_NCLOB", "SQL_NUMERIC", "SQL_NVARCHAR", "SQL_REAL",
            "SQL_SMALLINT", "SQL_TIME", "SQL_TIMESTAMP", "SQL_TINYINT", "SQL_TSI_DAY", "SQL_TSI_FRAC_SECOND",
            "SQL_TSI_HOUR", "SQL_TSI_MICROSECOND", "SQL_TSI_MINUTE", "SQL_TSI_MONTH", "SQL_TSI_QUARTER",
            "SQL_TSI_SECOND", "SQL_TSI_WEEK", "SQL_TSI_YEAR", "SQL_VARBINARY", "SQL_VARCHAR", "SQRT", "START", "STATE",
            "STATEMENT", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "STREAM", "STRUCTURE", "STYLE", "SUBCLASS_ORIGIN",
            "SUBMULTISET", "SUBSET", "SUBSTITUTE", "SUBSTRING", "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SYMMETRIC",
            "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TABLE_NAME", "TEMPORARY", "THEN", "TIES",
            "TIME", "TIMESTAMP", "TIMESTAMPADD", "TIMESTAMPDIFF", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TINYINT", "TO",
            "TOP_LEVEL_COUNT", "TRAILING", "TRANSACTION", "TRANSACTIONS_ACTIVE", "TRANSACTIONS_COMMITTED",
            "TRANSACTIONS_ROLLED_BACK", "TRANSFORM", "TRANSFORMS", "TRANSLATE", "TRANSLATE_REGEX", "TRANSLATION",
            "TREAT", "TRIGGER", "TRIGGER_CATALOG", "TRIGGER_NAME", "TRIGGER_SCHEMA", "TRIM", "TRIM_ARRAY", "TRUE",
            "TRUNCATE", "TYPE", "UESCAPE", "UNBOUNDED", "UNCOMMITTED", "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNAMED",
            "UNNEST", "UPDATE", "UPPER", "UPSERT", "USAGE", "USER", "USER_DEFINED_TYPE_CATALOG",
            "USER_DEFINED_TYPE_CODE", "USER_DEFINED_TYPE_NAME", "USER_DEFINED_TYPE_SCHEMA", "USING", "VALUE", "VALUES",
            "VALUE_OF", "VARBINARY", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "VERSION", "VERSIONING", "VIEW",
            "WEEK", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK",
            "WRAPPER", "WRITE", "XML", "YEAR", "ZONE");

    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Keywords,Non-reservedKeywordsandReservedKeywords
    public static final ImmutableSet<String> HIVE = ImmutableSet.of("ADD", "ADMIN", "AFTER", "ANALYZE", "ARCHIVE",
            "ASC", "BEFORE", "BUCKET", "BUCKETS", "CASCADE", "CHANGE", "CLUSTER", "CLUSTERED", "CLUSTERSTATUS",
            "COLLECTION", "COLUMNS", "COMMENT", "COMPACT", "COMPACTIONS", "COMPUTE", "CONCATENATE", "CONTINUE", "DATA",
            "DATABASES", "DATETIME", "DAY", "DBPROPERTIES", "DEFERRED", "DEFINED", "DELIMITED", "DEPENDENCY", "DESC",
            "DIRECTORIES", "DIRECTORY", "DISABLE", "DISTRIBUTE", "ELEM_TYPE", "ENABLE", "ESCAPED", "EXCLUSIVE",
            "EXPLAIN", "EXPORT", "FIELDS", "FILE", "FILEFORMAT", "FIRST", "FORMAT", "FORMATTED", "FUNCTIONS",
            "HOLD_DDLTIME", "HOUR", "IDXPROPERTIES", "IGNORE", "INDEX", "INDEXES", "INPATH", "INPUTDRIVER",
            "INPUTFORMAT", "ITEMS", "JAR", "KEYS", "KEY_TYPE", "LIMIT", "LINES", "LOAD", "LOCATION", "LOCK", "LOCKS",
            "LOGICAL", "LONG", "MAPJOIN", "MATERIALIZED", "METADATA", "MINUS", "MINUTE", "MONTH", "MSCK", "NOSCAN",
            "NO_DROP", "OFFLINE", "OPTION", "OUTPUTDRIVER", "OUTPUTFORMAT", "OVERWRITE", "OWNER", "PARTITIONED",
            "PARTITIONS", "PLUS", "PRETTY", "PRINCIPALS", "PROTECTION", "PURGE", "READ", "READONLY", "REBUILD",
            "RECORDREADER", "RECORDWRITER", "REGEXP", "RELOAD", "RENAME", "REPAIR", "REPLACE", "REPLICATION",
            "RESTRICT", "REWRITE", "RLIKE", "ROLE", "ROLES", "SCHEMA", "SCHEMAS", "SECOND", "SEMI", "SERDE",
            "SERDEPROPERTIES", "SERVER", "SETS", "SHARED", "SHOW", "SHOW_DATABASE", "SKEWED", "SORT", "SORTED", "SSL",
            "STATISTICS", "STORED", "STREAMTABLE", "STRING", "STRUCT", "TABLES", "TBLPROPERTIES", "TEMPORARY",
            "TERMINATED", "TINYINT", "TOUCH", "TRANSACTIONS", "UNARCHIVE", "UNDO", "UNIONTYPE", "UNLOCK", "UNSET",
            "UNSIGNED", "URI", "USE", "UTC", "UTCTIMESTAMP", "VALUE_TYPE", "VIEW", "WHILE", "YEAR", "AUTOCOMMIT",
            "ISOLATION", "LEVEL", "OFFSET", "SNAPSHOT", "TRANSACTION", "WORK", "WRITE", "COMMIT", "ONLY", "REGEXP",
            "RLIKE", "ROLLBACK", "START", "ABORT", "KEY", "LAST", "NORELY", "NOVALIDATE", "NULLS", "RELY", "VALIDATE",
            "CACHE", "CONSTRAINT", "FOREIGN", "PRIMARY", "REFERENCES", "DETAIL", "DOW", "EXPRESSION", "OPERATOR",
            "QUARTER", "SUMMARY", "VECTORIZATION", "WEEK", "YEARS", "MONTHS", "WEEKS", "DAYS", "HOURS", "MINUTES",
            "SECONDS", "DAYOFWEEK", "EXTRACT", "FLOOR", "INTEGER", "PRECISION", "VIEWS", "TIMESTAMPTZ", "ZONE", "TIME",
            "NUMERIC");
}
