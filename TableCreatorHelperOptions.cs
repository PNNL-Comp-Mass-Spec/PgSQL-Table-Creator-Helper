﻿using System;
using System.Reflection;
using PRISM;

namespace PgSqlTableCreatorHelper
{
    public class TableCreatorHelperOptions
    {
        // Ignore Spelling: app, pre, Sql

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "January 23, 2025";

        [Option("Input", "I", ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "SQL script file to process")]
        public string InputScriptFile { get; set; }

        [Option("Map", "M", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Column name map file (typically created by sqlserver2pgsql.pl)\n" +
                       "Tab-delimited file with five columns:\n" +
                       "SourceTable  SourceName  Schema  NewTable  NewName")]
        public string ColumnNameMapFile { get; set; }

        [Option("Map2", "AltMap", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Alternative column name map file\n" +
                       "(typically sent to DB_Schema_Export_Tool.exe via the ColumnMap parameter when using the ExistingDDL option " +
                       "to pre-process a DDL file prior to calling sqlserver2pgsql.pl)\n" +
                       "Tab-delimited file with three columns:\n" +
                       "SourceTableName  SourceColumnName  TargetColumnName")]
        public string ColumnNameMapFile2 { get; set; }

        [Option("TableNameMap", "TableNames", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file with table names (one name per line) used to track renamed tables\n" +
                       "(typically sent to DB_Schema_Export_Tool.exe via the DataTables parameter when using the ExistingDDL option " +
                       "to pre-process a DDL file prior to calling sqlserver2pgsql.pl)\n" +
                       "Tab-delimited file that must include columns SourceTableName and TargetTableName")]
        public string TableNameMapFile { get; set; }

        [Option("DefaultSchema", "Schema", HelpShowsDefault = false,
            HelpText = "Schema to prefix table names with (when the name does not have a schema)")]
        public string DefaultSchema { get; set; }

        [Option("Verbose", "V", HelpShowsDefault = true,
            HelpText = "When true, display the old and new version of each updated line")]
        public bool VerboseOutput { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public TableCreatorHelperOptions()
        {
            InputScriptFile = string.Empty;
            ColumnNameMapFile = string.Empty;
            ColumnNameMapFile2 = string.Empty;
            TableNameMapFile = string.Empty;
            DefaultSchema = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");

            Console.WriteLine(" {0,-35} {1}", "Input script file:", PathUtils.CompactPathString(InputScriptFile, 80));

            Console.WriteLine(" {0,-35} {1}", "Column name map file:", PathUtils.CompactPathString(ColumnNameMapFile, 80));

            if (!string.IsNullOrWhiteSpace(ColumnNameMapFile2))
            {
                Console.WriteLine(" {0,-35} {1}", "Secondary column name map file:", PathUtils.CompactPathString(ColumnNameMapFile2, 80));
            }

            if (!string.IsNullOrWhiteSpace(TableNameMapFile))
            {
                Console.WriteLine(" {0,-35} {1}", "Table name map file:", PathUtils.CompactPathString(TableNameMapFile, 80));
            }

            Console.WriteLine(" {0,-35} {1}", "Default schema name:",
                string.IsNullOrWhiteSpace(DefaultSchema) ? "not defined" : DefaultSchema);

            Console.WriteLine(" {0,-35} {1}", "Verbose Output:", VerboseOutput);

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns>True if options are valid, false if /I or /M is missing</returns>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(InputScriptFile))
            {
                errorMessage = "Use /I to specify the SQL script file to process";
                return false;
            }

            if (string.IsNullOrWhiteSpace(ColumnNameMapFile))
            {
                errorMessage = "Use /M to specify the column name map file";
                return false;
            }

            errorMessage = string.Empty;

            return true;
        }
    }
}
