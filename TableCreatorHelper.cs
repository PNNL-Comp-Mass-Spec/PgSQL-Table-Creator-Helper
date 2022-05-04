using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;
using TableColumnNameMapContainer;

namespace PgSqlTableCreatorHelper
{
    public class TableCreatorHelper : EventNotifier
    {
        // Ignore Spelling: dbo, dms, dpkg, mc, ont, sw

        /// <summary>
        /// Match any lowercase letter
        /// </summary>
        private readonly Regex mAnyLowerMatcher;

        /// <summary>
        /// Match a lowercase letter followed by an uppercase letter
        /// </summary>
        private readonly Regex mCamelCaseMatcher;

        private readonly TableCreatorHelperOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public TableCreatorHelper(TableCreatorHelperOptions options)
        {
            mOptions = options;

            mAnyLowerMatcher = new Regex("[a-z]", RegexOptions.Compiled | RegexOptions.Singleline);

            mCamelCaseMatcher = new Regex("(?<LowerLetter>[a-z])(?<UpperLetter>[A-Z])", RegexOptions.Compiled);
        }

        /// <summary>
        /// Convert the object name to snake_case
        /// </summary>
        /// <param name="objectName"></param>
        private string ConvertNameToSnakeCase(string objectName)
        {
            if (!mAnyLowerMatcher.IsMatch(objectName))
            {
                // objectName contains no lowercase letters; simply change to lowercase and return
                return objectName.ToLower();
            }

            var match = mCamelCaseMatcher.Match(objectName);

            var updatedName = match.Success
                ? mCamelCaseMatcher.Replace(objectName, "${LowerLetter}_${UpperLetter}")
                : objectName;

            return updatedName.ToLower();
        }

        private List<string> GetColumnNames(Group columnMatchGroup)
        {
            if (!columnMatchGroup.Success)
                return new List<string>();

            var columnList = columnMatchGroup.Value.Split(',');

            var columnNames = new List<string>();

            foreach (var item in columnList)
            {
                string quotedName;

                if (item.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase))
                    quotedName = item.Substring(0, item.Length - 3).Trim();
                else if (item.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase))
                    quotedName = item.Substring(0, item.Length - 4).Trim();
                else
                    quotedName = item;

                columnNames.Add(quotedName.Replace("\"", string.Empty));
            }

            return columnNames;
        }

        /// <summary>
        /// Get the object name, without the schema
        /// </summary>
        /// <remarks>
        /// Simply looks for the first period and assumes the schema name is before the period and the object name is after it
        /// </remarks>
        /// <param name="objectName"></param>
        private string GetNameWithoutSchema(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return string.Empty;

            var periodIndex = objectName.IndexOf('.');
            if (periodIndex > 0 && periodIndex < objectName.Length - 1)
                return objectName.Substring(periodIndex + 1);

            return objectName;
        }

        private string GetUniqueName(string itemName, string tableName, IDictionary<string, string> nameToTableMap, bool showWarning = true)
        {
            if (itemName.Length >= MAX_OBJECT_NAME_LENGTH)
            {
                itemName = TruncateString(itemName, MAX_OBJECT_NAME_LENGTH - 1);

                if (showWarning)
                {
                    OnWarningEvent(
                        "Object name on table {0} is longer than {1} characters; truncating to:\n    {2}",
                        tableName, MAX_OBJECT_NAME_LENGTH, itemName);
                }
            }

            for (var addOn = 2; nameToTableMap.ContainsKey(itemName); addOn++)
            {
                if (nameToTableMap.ContainsKey(itemName + addOn))
                    continue;

                itemName += addOn;
                nameToTableMap.Add(itemName, tableName);
                break;
            }

            return itemName;
        }

        private bool LoadNameMapFiles(out Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap)
        {
            var columnMapFile = new FileInfo(mOptions.ColumnNameMapFile);

            if (!columnMapFile.Exists)
            {
                OnErrorEvent("Column name map file not found: " + columnMapFile.FullName);
                columnNameMap = new Dictionary<string, Dictionary<string, WordReplacer>>();
                return false;
            }

            var mapReader = new NameMapReader();
            RegisterEvents(mapReader);

            // In dictionary tableNameMap, keys are the original (source) table names
            // and values are WordReplacer classes that track the new table names and new column names in PostgreSQL

            // In dictionary columnNameMap, keys are new table names
            // and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
            // names should not have double quotes around them

            // Dictionary tableNameMapSynonyms mas original table names to new table names

            var columnMapFileLoaded = mapReader.LoadSqlServerToPgSqlColumnMapFile(
                columnMapFile,
                mOptions.DefaultSchema,
                true,
                out var tableNameMap,
                out columnNameMap);

            if (!columnMapFileLoaded)
                return false;

            var tableNameMapSynonyms = new Dictionary<string, string>();

            if (!string.IsNullOrWhiteSpace(mOptions.TableNameMapFile))
            {
                var tableNameMapFile = new FileInfo(mOptions.TableNameMapFile);
                if (!tableNameMapFile.Exists)
                {
                    OnErrorEvent("Table name map file not found: " + tableNameMapFile.FullName);
                    return false;
                }

                var tableNameMapReader = new TableNameMapContainer.NameMapReader();
                RegisterEvents(tableNameMapReader);

                var tableNameInfo = tableNameMapReader.LoadTableNameMapFile(tableNameMapFile.FullName, true, out var abortProcessing);

                if (abortProcessing)
                {
                    return false;
                }

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var item in tableNameInfo)
                {
                    if (tableNameMapSynonyms.ContainsKey(item.SourceTableName) || string.IsNullOrWhiteSpace(item.TargetTableName))
                        continue;

                    tableNameMapSynonyms.Add(item.SourceTableName, item.TargetTableName);
                }
            }

            if (string.IsNullOrWhiteSpace(mOptions.ColumnNameMapFile2))
                return true;

            var columnMapFile2 = new FileInfo(mOptions.ColumnNameMapFile2);
            if (!columnMapFile2.Exists)
            {
                OnErrorEvent("Secondary column name map file not found: " + columnMapFile2.FullName);
                return false;
            }

            var secondaryMapFileLoaded = mapReader.LoadTableColumnMapFile(columnMapFile2, tableNameMap, columnNameMap, tableNameMapSynonyms);

            if (!secondaryMapFileLoaded)
                return false;

            // When Perl script sqlserver2pgsql.pl writes out CREATE INDEX lines, the include columns are converted to snake case but are not renamed
            // To account for this, step through the tables and columns in columnNameMap and add snake case mappings
            foreach (var tableItem in columnNameMap)
            {
                var currentTable = tableItem.Key;

                var columnsToAdd = new Dictionary<string, WordReplacer>();

                foreach (var columnItem in tableItem.Value)
                {
                    var updatedColumnName = ConvertNameToSnakeCase(columnItem.Key);

                    if (updatedColumnName.Equals(columnItem.Key, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (updatedColumnName.Equals(columnItem.Value.ReplacementText))
                        continue;

                    columnsToAdd.Add(updatedColumnName,
                        new WordReplacer(updatedColumnName, columnItem.Value.ReplacementText, columnItem.Value.DefaultSchema));
                }

                foreach (var newColumn in columnsToAdd)
                {
                    if (!tableItem.Value.ContainsKey(newColumn.Key))
                    {
                        tableItem.Value.Add(newColumn.Key, newColumn.Value);
                    }
                    else
                    {
                        OnDebugEvent("Table {0} already has the mapping {1} -> {2}", currentTable, newColumn.Key, newColumn.Value);
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Process the input file
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool ProcessInputFile()
        {
            try
            {
                var inputFile = new FileInfo(mOptions.InputScriptFile);
                if (!inputFile.Exists)
                {
                    OnErrorEvent("Input file not found: " + inputFile.FullName);
                    return false;
                }

                if (inputFile.Directory == null || inputFile.DirectoryName == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of the input file: " + inputFile.FullName);
                    return false;
                }

                var outputFilePath = Path.Combine(
                    inputFile.DirectoryName,
                    Path.GetFileNameWithoutExtension(inputFile.Name) + "_updated" + inputFile.Extension);

                // Load the various name map files
                if (!LoadNameMapFiles(out var columnNameMap))
                    return false;


                var addConstraintMatcher = new Regex(
                    "^ *ALTER TABLE (?<TableName>.+?) ADD CONSTRAINT (?<ConstraintName>.+?) (?<ConstraintType>UNIQUE|PRIMARY KEY)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var indexMatcherWithInclude = new Regex(
                    @"^ *CREATE (UNIQUE )?INDEX (?<IndexName>.+?) ON (?<TableNameWithSchema>.+?) \((?<IndexedColumns>.+?)\) INCLUDE \((?<IncludedColumns>.+)\)(?<Suffix>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var indexMatcher = new Regex(
                    @"^ *CREATE (UNIQUE )?INDEX (?<IndexName>.+?) ON (?<TableNameWithSchema>.+?) \((?<IndexedColumns>.+?)\)(?<Suffix>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Keys in this dictionary are index or constraint name, values are the table the index or constraint applies to
                var indexAndConstraintNames = new Dictionary<string, string>();

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

                // The default search path is: "$user", public
                // The DMS database customizes this, as shown here

                // Command "SET search_path" updates the search path for the current session

                // To update permanently, use:
                // ALTER DATABASE dms SET search_path=public, sw, cap, dpkg, mc, ont;

                writer.WriteLine("SET search_path TO public, sw, cap, dpkg, mc, ont;");
                writer.WriteLine("SHOW search_path;");
                writer.WriteLine();

                var rowsProcessed = 0;

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var constraintMatch = addConstraintMatcher.Match(dataLine);

                    if (constraintMatch.Success)
                    {
                        var constraintName = constraintMatch.Groups["ConstraintName"].Value;
                        var tableName = constraintMatch.Groups["TableName"].Value;

                        indexAndConstraintNames.Add(TrimQuotes(constraintName), TrimQuotes(GetNameWithoutSchema(tableName)));

                        writer.WriteLine(dataLine);
                        continue;
                    }

                    Match indexMatch;

                    var indexWithIncludeMatch = indexMatcherWithInclude.Match(dataLine);

                    if (indexWithIncludeMatch.Success)
                    {
                        indexMatch = indexWithIncludeMatch;
                    }
                    else
                    {
                        var indexMatchNoInclude = indexMatcher.Match(dataLine);

                        indexMatch = indexMatchNoInclude.Success ? indexMatchNoInclude : null;
                    }

                    if (indexMatch == null)
                    {
                        writer.WriteLine(dataLine);
                        continue;
                    }

                    var success = ProcessCreateIndexLine(dataLine, indexMatch, columnNameMap, writer, indexAndConstraintNames);
                    if (!success)
                        return false;

                    rowsProcessed++;
                }

                writer.WriteLine();

                writer.WriteLine("Processed {0} lines in the input file", rowsProcessed);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessInputFile", ex);
                return false;
            }
        }

        /// <summary>
        /// Updates column names in the CREATE INDEX line
        /// Writes the updated text to disk
        /// </summary>
        /// <param name="sourceDataLine"></param>
        /// <param name="indexMatch"></param>
        /// <param name="columnNameMap">
        /// Dictionary where keys are new table names
        /// and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
        /// names should not have double quotes around them
        /// </param>
        /// <param name="writer">
        /// Output file writer</param>
        /// <param name="indexAndConstraintNames">
        /// Keys in this dictionary are constraint or index name, values are the table the constraint or index applies to
        /// </param>
        private bool ProcessCreateIndexLine(
            string sourceDataLine,
            Match indexMatch,
            Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap,
            TextWriter writer,
            IDictionary<string, string> indexAndConstraintNames)
        {
            var referencedTables = new SortedSet<string>();

            var indexName = indexMatch.Groups["IndexName"].Value;

            // Determine the table for this index
            var tableNameWithSchema = indexMatch.Groups["TableNameWithSchema"].Value;

            var tableName = TrimQuotes(GetNameWithoutSchema(tableNameWithSchema));
            referencedTables.Add(tableName);

            var updatedLine = NameUpdater.UpdateColumnNames(columnNameMap, referencedTables, sourceDataLine, false);

            // Check for old column names in the index name

            foreach (var updatedTableName in referencedTables)
            {
                if (!columnNameMap.TryGetValue(updatedTableName, out var nameMapping))
                    continue;

                var tableNameIndex = indexName.IndexOf(tableName, StringComparison.OrdinalIgnoreCase);

                var indexNamePortion = tableNameIndex < 0
                    ? indexName
                    : indexName.Substring(tableNameIndex + tableName.Length);

                foreach (var item  in nameMapping.Values)
                {
                    if (item.TextToFind.Equals("ID", StringComparison.OrdinalIgnoreCase))
                    {
                        // This name is too generic; don't replace anything
                        continue;
                    }

                    var columnNameMatcher = new Regex(string.Format(@"(?<Prefix>\b|_){0}(?<Suffix>\b|_)", item.TextToFind), RegexOptions.Compiled | RegexOptions.IgnoreCase);

                    var match = columnNameMatcher.Match(indexNamePortion);

                    if (!match.Success)
                        continue;

                    if (match.Value.Trim('_').Equals(item.ReplacementText, StringComparison.OrdinalIgnoreCase))
                    {
                        // No actual change (besides, possibly case)
                        continue;
                    }

                    var updatedIndexNamePart = string.Format("{0}{1}{2}", match.Groups["Prefix"], item.ReplacementText, match.Groups["Suffix"]);

                    indexNamePortion = columnNameMatcher.Replace(indexNamePortion, updatedIndexNamePart);

                    if (tableNameIndex < 0)
                        indexName = indexNamePortion;
                    else
                        indexName = indexName.Substring(0, tableNameIndex + tableName.Length) + indexNamePortion;
                }
            }

            var indexNameNoQuotes = TrimQuotes(indexName);

            if (indexAndConstraintNames.TryGetValue(indexNameNoQuotes, out var otherTableName))
            {
                if (tableName.Equals(otherTableName, StringComparison.OrdinalIgnoreCase))
                {
                    OnWarningEvent("Duplicate index name found on table {0}: {1}", tableName, indexNameNoQuotes);
                }
                else
                {
                    OnWarningEvent("Duplicate index name found on table {0}: {1} is also on {2}", tableName, indexNameNoQuotes, otherTableName);
                }

                var indexedColumns = GetColumnNames(indexMatch.Groups["IndexedColumns"]);
                var includedColumns = GetColumnNames(indexMatch.Groups["IncludedColumns"]);

                var nameUpdateRequired = true;

                foreach (var indexedColumn in indexedColumns)
                {
                    indexNameNoQuotes += string.Format("_{0}", indexedColumn);

                    if (indexAndConstraintNames.ContainsKey(indexNameNoQuotes))
                        continue;

                    indexAndConstraintNames.Add(indexNameNoQuotes, tableName);
                    nameUpdateRequired = false;
                    break;
                }

                if (nameUpdateRequired && includedColumns.Count > 0)
                {
                    indexNameNoQuotes += "_include";

                    foreach (var includedColumn in includedColumns)
                    {
                        indexNameNoQuotes += string.Format("_{0}", includedColumn);

                        if (indexAndConstraintNames.ContainsKey(indexNameNoQuotes))
                            continue;

                        indexAndConstraintNames.Add(indexNameNoQuotes, tableName);
                        nameUpdateRequired = false;
                        break;
                    }
                }

                if (nameUpdateRequired)
                {
                    // Append an integer to generate a unique name
                    indexNameNoQuotes = GetUniqueName(indexNameNoQuotes, tableName, indexAndConstraintNames);
                }

                // Update indexName, surrounding with double quotes
                indexName = string.Format("\"{0}\"", indexNameNoQuotes);
            }
            else
            {
                indexAndConstraintNames.Add(indexNameNoQuotes, tableName);
            }

            if (!indexMatch.Groups["IndexName"].Value.Equals(indexName))
            {
                OnDebugEvent("Renaming index from {0}\n{1}to {2}",
                    indexMatch.Groups["IndexName"].Value,
                    new string(' ', 19),
                    indexName);

                updatedLine = updatedLine.Replace(indexMatch.Groups["IndexName"].Value, indexName);
            }

            if (sourceDataLine.Equals(updatedLine))
            {
                writer.WriteLine(sourceDataLine);
                return true;
            }

            if (mOptions.VerboseOutput)
            {
                OnDebugEvent("Updating {0} \n        to {1}", sourceDataLine, updatedLine);
            }

            writer.WriteLine(updatedLine);

            return true;
        }

        /// <summary>
        /// If the object name begins and ends with double quotes, remove them
        /// </summary>
        /// <param name="objectName"></param>
        private static string TrimQuotes(string objectName)
        {
            if (objectName.StartsWith("\"") && objectName.EndsWith("\""))
            {
                return objectName.Substring(1, objectName.Length - 2);
            }

            return objectName;
        }
    }
}
