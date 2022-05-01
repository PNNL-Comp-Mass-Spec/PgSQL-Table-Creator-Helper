# PgSQL Table Creator Helper

This program processes a SQL DDL file with `CREATE INDEX` commands and renames the
column names referenced by the DDL statements to use new names defined in a
mapping file (or files).

This program is derived from the PgSQL View Creator Helper program, available at
* https://github.com/PNNL-Comp-Mass-Spec/PgSQL-View-Creator-Helper

## Console Switches

PgSqlTableCreatorHelper is a console application, and must be run from the Windows command prompt.

```
PgSqlTableCreatorHelper.exe
  /I:InputFilePath
  /Map:ColumnNameMapFile
  [/Map2:SecondaryColumnMapFile]
  [/TableNameMap:TableNameMapFile]
  [/Schema:DefaultSchemaName]
  [/V]
  [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

The input file should be a SQL text file with `CREATE INDEX` statements
* The program is primarily designed to update the `DBName_after.sql` file created by Perl script `sqlserver2pgsql.pl` (https://github.com/PNNL-Comp-Mass-Spec/sqlserver2pgsql) 
* Example input file statements:

```PLpgSQL
CREATE INDEX "ix_t_analysis_job_aj_last_affected" ON "public"."t_analysis_job" ("last_affected" ASC);
CREATE INDEX "ix_t_analysis_job_aj_state_id_aj_job_id" ON "public"."t_analysis_job" ("job_state_id" ASC,"job" ASC);
CREATE INDEX "ix_t_analysis_job_aj_state_name_cached" ON "public"."t_analysis_job" ("state_name_cached" ASC);
CREATE INDEX "ix_t_analysis_job_aj_dataset_id" ON "public"."t_analysis_job" ("dataset_id" ASC);
CREATE INDEX "ix_t_analysis_job_batch_id_include_job_id" ON "public"."t_analysis_job" ("batch_id" ASC) INCLUDE ("job");
CREATE INDEX "ix_t_analysis_job_created_include_job_state_id_progress" ON "public"."t_analysis_job" ("created" ASC) INCLUDE ("job","aj_state_id","progress");
CREATE INDEX "ix_t_analysis_job_request_id" ON "public"."t_analysis_job" ("request_id" ASC);
CREATE INDEX "ix_t_analysis_job_started_include_job_state_id_progress" ON "public"."t_analysis_job" ("start" ASC) INCLUDE ("job","aj_state_id","progress");
CREATE INDEX "ix_t_analysis_job_state_id_include_job_priority_tool_dataset" ON "public"."t_analysis_job" ("job_state_id" ASC) INCLUDE ("priority","aj_job_id","aj_dataset_id","aj_analysis_tool_id");

CREATE UNIQUE INDEX "ix_t_users_u_name" ON "public"."t_users" ("username" ASC);
CREATE INDEX "ix_t_biomaterial_ccname_container_id_ccid" ON "public"."t_biomaterial" ("biomaterial_name" ASC,"container_id" ASC,"biomaterial_id" ASC);
CREATE INDEX "ix_t_biomaterial_cc_campaign_id" ON "public"."t_biomaterial" ("campaign_id" ASC);
CREATE INDEX "ix_t_biomaterial_cc_created" ON "public"."t_biomaterial" ("created" ASC);
CREATE UNIQUE INDEX "ix_t_biomaterial_cc_name" ON "public"."t_biomaterial" ("biomaterial_name" ASC);
```

* Example output file statements:
```PLpgSQL
CREATE INDEX "ix_t_analysis_job_last_affected" ON "public"."t_analysis_job" ("last_affected" ASC);
CREATE INDEX "ix_t_analysis_job_job_state_id_job" ON "public"."t_analysis_job" ("job_state_id" ASC,"job" ASC);
CREATE INDEX "ix_t_analysis_job_state_name_cached" ON "public"."t_analysis_job" ("state_name_cached" ASC);
CREATE INDEX "ix_t_analysis_job_dataset_id" ON "public"."t_analysis_job" ("dataset_id" ASC);
CREATE INDEX "ix_t_analysis_job_batch_id_include_job_id" ON "public"."t_analysis_job" ("batch_id" ASC) INCLUDE ("job");
CREATE INDEX "ix_t_analysis_job_created_include_job_state_id_progress" ON "public"."t_analysis_job" ("created" ASC) INCLUDE ("job","job_state_id","progress");
CREATE INDEX "ix_t_analysis_job_request_id" ON "public"."t_analysis_job" ("request_id" ASC);
CREATE INDEX "ix_t_analysis_job_started_include_job_state_id_progress" ON "public"."t_analysis_job" ("start" ASC) INCLUDE ("job","job_state_id","progress");
CREATE INDEX "ix_t_analysis_job_state_id_include_job_priority_tool_dataset" ON "public"."t_analysis_job" ("job_state_id" ASC) INCLUDE ("priority","job","dataset_id","analysis_tool_id");

CREATE UNIQUE INDEX "ix_t_users_username" ON "public"."t_users" ("username" ASC);
CREATE INDEX "ix_t_biomaterial_ccname_container_id_ccid" ON "public"."t_biomaterial" ("biomaterial_name" ASC,"container_id" ASC,"biomaterial_id" ASC);
CREATE INDEX "ix_t_biomaterial_campaign_id" ON "public"."t_biomaterial" ("campaign_id" ASC);
CREATE INDEX "ix_t_biomaterial_created" ON "public"."t_biomaterial" ("created" ASC);
CREATE UNIQUE INDEX "ix_t_biomaterial_biomaterial_name" ON "public"."t_biomaterial" ("biomaterial_name" ASC);
```


The `/Map` file is is a tab-delimited text file with five columns
* The Map file matches the format of the NameMap file created by Perl script sqlserver2pgsql.pl
* Example data:

| SourceTable   | SourceName           | Schema | NewTable        | NewName                 |
|---------------|----------------------|--------|-----------------|-------------------------|
| T_Log_Entries | Entry_ID             | mc     | "t_log_entries" | "entry_id"              |
| T_Log_Entries | posted_by            | mc     | "t_log_entries" | "posted_by"             |
| T_Log_Entries | posting_time         | mc     | "t_log_entries" | "posting_time"          |
| T_Log_Entries | type                 | mc     | "t_log_entries" | "type"                  |
| T_Log_Entries | message              | mc     | "t_log_entries" | "message"               |
| T_Log_Entries | Entered_By           | mc     | "t_log_entries" | "entered_by"            |
| T_Mgrs        | m_id                 | mc     | "t_mgrs"        | "mgr_id"                |
| T_Mgrs        | m_name               | mc     | "t_mgrs"        | "mgr_name"              |
| T_Mgrs        | mgr_type_id          | mc     | "t_mgrs"        | "mgr_type_id"           |
| T_Mgrs        | param_value_changed  | mc     | "t_mgrs"        | "param_value_changed"   |
| T_Mgrs        | control_from_website | mc     | "t_mgrs"        | "control_from_website"  |
| T_Mgrs        | comment              | mc     | "t_mgrs"        | "comment"               |


Use `/Map2` to optionally specify a secondary map file, which is a tab-delimited text file with three columns
* The Secondary Map file matches the file defined for the `ColumnMap` parameter when using the DB Schema Export Tool (https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool) to pre-process an existing DDL file
* Example data:

| SourceTableName         | SourceColumnName     | TargetColumnName     |
|-------------------------|----------------------|----------------------|
| T_MgrTypes              | mt_typeid            | mgr_type_id          |
| T_MgrTypes              | mt_typename          | mgr_type_name        |
| T_MgrTypes              | mt_active            | mgr_type_active      |
| T_MgrType_ParamType_Map | MgrTypeID            | mgr_type_id          |
| T_MgrType_ParamType_Map | ParamTypeID          | param_type_id        |
| T_Mgrs                  | M_ID                 | mgr_id               |
| T_Mgrs                  | M_Name               | mgr_name             |
| T_Mgrs                  | M_TypeID             | mgr_type_id          |
| T_Mgrs                  | M_ParmValueChanged   | param_value_changed  |
| T_Mgrs                  | M_ControlFromWebsite | control_from_website |
| T_Mgrs                  | M_Comment            | comment              |


Use `/TableNameMap` (or `/TableNames`) to optionally specify a tab-delimited text file listing old and new names for renamed tables
* The Table Name Map file matches the file defined for the `DataTables` parameter when using the DB Schema Export Tool (https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool) to pre-process an existing DDL file
  * The text file must include columns `SourceTableName` and `TargetTableName`
* Example data (showing additional columns that are used by the DB Schema Export Tool, but are ignored by this program)

| SourceTableName        | TargetSchemaName | TargetTableName       | PgInsert  | KeyColumn(s)      |
|------------------------|------------------|-----------------------|-----------|-------------------|
| T_Analysis_State_Name  | public           | t_analysis_job_state  | true      | job_state_id      |
| T_DatasetRatingName    | public           | t_dataset_rating_name | true      | dataset_rating_id |
| T_Log_Entries          | public           | t_log_entries         | false     |                   |
| T_Job_Events           | cap              | t_job_Events          | false     |                   |
| T_Job_State_Name       | cap              | t_job_state_name      | true      | job               |
| T_Users                | public           | t_users               | true      | user_id           |


Use `/Schema` to specify a default schema name to add before all table names (that don't already have a schema name prefix)

Use `/V` to enable verbose mode, displaying the old and new version of each updated line

The processing options can be specified in a parameter file using `/ParamFile:Options.conf` or `/Conf:Options.conf`
* Define options using the format `ArgumentName=Value`
* Lines starting with `#` or `;` will be treated as comments
* Additional arguments on the command line can supplement or override the arguments in the parameter file

Use `/CreateParamFile` to create an example parameter file
* By default, the example parameter file content is shown at the console
* To create a file named Options.conf, use `/CreateParamFile:Options.conf`

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics/
Source code: https://github.com/PNNL-Comp-Mass-Spec/PgSQL-Table-Creator-Helper

## License

Licensed under the 2-Clause BSD License; you may not use this program except
in compliance with the License.  You may obtain a copy of the License at
https://opensource.org/licenses/BSD-2-Clause

Copyright 2022 Battelle Memorial Institute
