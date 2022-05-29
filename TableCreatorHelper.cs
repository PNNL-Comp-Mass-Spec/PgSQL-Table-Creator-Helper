using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PRISM;
using TableColumnNameMapContainer;

namespace PgSqlTableCreatorHelper
{
    public class TableCreatorHelper : EventNotifier
    {
        // Ignore Spelling: dbo, dms, dpkg, mc, Postgres, ont, sw

        /// <summary>
        /// Maximum length of object names in PostgreSQL
        /// </summary>
        private const int MAX_OBJECT_NAME_LENGTH = 63;

        private readonly TableCreatorHelperOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public TableCreatorHelper(TableCreatorHelperOptions options)
        {
            mOptions = options;
        }

        /// <summary>
        /// Determine the actual foreign key name to use for each foreign key
        /// </summary>
        /// <param name="truncatedForeignKeyMap">
        /// Keys in this dictionary are foreign key names, truncated to 63 characters
        /// Values are the full key names that correspond to each truncated name
        /// </param>
        /// <returns>
        /// Dictionary where Keys are the original, full length foreign key name and values are the actual name to use (possibly truncated)
        /// </returns>
        private Dictionary<string, string> ConstructForeignKeyMap(Dictionary<string, List<string>> truncatedForeignKeyMap)
        {
            var foreignKeyNameMap = new Dictionary<string, string>();

            foreach (var truncatedForeignKey in truncatedForeignKeyMap)
            {
                var truncatedName = truncatedForeignKey.Key;

                if (truncatedForeignKey.Value.Count > 1)
                {
                    // Multiple foreign keys have been truncated to the same value
                    // Sequentially number them

                    var i = 1;
                    foreach (var originalKeyName in truncatedForeignKey.Value)
                    {
                        foreignKeyNameMap.Add(originalKeyName, string.Format("{0}{1}", truncatedName, i));
                        i++;
                    }

                    continue;
                }

                // Only one foreign key; append to the dictionary
                foreignKeyNameMap.Add(truncatedForeignKey.Value[0], truncatedName);
            }

            return foreignKeyNameMap;
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
        private static string GetNameWithoutSchema(string objectName)
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

            // Dictionary tableNameMapSynonyms has original table names to new table names

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
                    var updatedColumnName = NameUpdater.ConvertNameToSnakeCase(columnItem.Key);

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

                // Scan the input file to look for foreign key name conflicts, considering the 63 character limit
                if (!ResolveForeignKeyNameConflicts(inputFile, out var foreignKeyRenames))
                    return false;

                var addConstraintMatcher = new Regex(
                    @"^ *ALTER TABLE (?<TableName>.+?) ADD CONSTRAINT (?<ConstraintName>.+?) (?<ConstraintType>UNIQUE|PRIMARY KEY|FOREIGN KEY) \((?<TargetColumn>.+?)\)",
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
                        var constraintNameNoQuotes = TrimQuotes(constraintName);

                        var tableName = constraintMatch.Groups["TableName"].Value;
                        var tableNameWithoutSchema = TrimQuotes(GetNameWithoutSchema(tableName));

                        var constraintType = constraintMatch.Groups["ConstraintType"].Value;

                        var targetColumn = constraintMatch.Groups["TargetColumn"].Value;

                        if (constraintType.Equals("FOREIGN KEY", StringComparison.OrdinalIgnoreCase))
                        {
                            if (foreignKeyRenames.TryGetValue(tableNameWithoutSchema, out var renamedForeignKeys) &&
                                renamedForeignKeys.TryGetValue(constraintNameNoQuotes, out var newForeignKeyName))
                            {
                                var updatedLine = dataLine.Replace(constraintNameNoQuotes, newForeignKeyName);
                                writer.WriteLine(updatedLine);
                                continue;
                            }
                        }
                        else
                        {
                            string constraintNameToExamine;
                            string constraintNameToUse;

                            if (constraintType.Equals("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) &&
                                !constraintNameNoQuotes.StartsWith("pk_", StringComparison.OrdinalIgnoreCase))
                            {
                                // Rename primary key constraints to start with pk_

                                if (constraintNameNoQuotes.EndsWith("_pk"))
                                {
                                    constraintNameToExamine = "pk_" + constraintNameNoQuotes.Substring(0, constraintNameNoQuotes.Length - 3);
                                }
                                else
                                {
                                    constraintNameToExamine = "pk_" + constraintNameNoQuotes;
                                }
                            }
                            else if (constraintType.Equals("UNIQUE", StringComparison.OrdinalIgnoreCase) &&
                                     constraintNameNoQuotes.Equals("ix_" + tableNameWithoutSchema, StringComparison.OrdinalIgnoreCase))
                            {
                                // SQL server uses the generic name IX_T_TableName for unique constraints
                                // Customize the constraint name
                                constraintNameToExamine = string.Format("{0}_unique_{1}", constraintNameNoQuotes, TrimQuotes(targetColumn));
                            }
                            else
                            {
                                constraintNameToExamine = constraintNameNoQuotes;
                            }

                            if (constraintNameToExamine.Length > MAX_OBJECT_NAME_LENGTH)
                            {
                                constraintNameToUse = TruncateString(constraintNameToExamine, MAX_OBJECT_NAME_LENGTH);

                                OnWarningEvent(
                                    "Constraint name is longer than {0} characters; truncating to: {1}",
                                    MAX_OBJECT_NAME_LENGTH, constraintNameToUse);
                            }
                            else
                            {
                                constraintNameToUse = constraintNameToExamine;
                            }

                            if (indexAndConstraintNames.ContainsKey(constraintNameToUse))
                            {
                                OnWarningEvent("Duplicate constraint name found on table {0}: {1}", tableName, constraintNameToUse);

                                // Append an integer to generate a unique name
                                var newConstraintName = GetUniqueName(constraintNameToUse, tableName, indexAndConstraintNames);

                                var updatedLine = dataLine.Replace(constraintNameNoQuotes, newConstraintName);
                                writer.WriteLine(updatedLine);

                                indexAndConstraintNames.Add(newConstraintName, tableNameWithoutSchema);
                                continue;
                            }

                            if (!constraintNameToUse.Equals(constraintNameNoQuotes))
                            {
                                OnWarningEvent("Renaming {0} constraint on table {1}: {2}",
                                    constraintType.ToLower(), tableName, constraintNameToUse);

                                var updatedLine = dataLine.Replace(constraintNameNoQuotes, constraintNameToUse);
                                writer.WriteLine(updatedLine);

                                indexAndConstraintNames.Add(constraintNameToUse, tableNameWithoutSchema);
                                continue;
                            }

                            indexAndConstraintNames.Add(constraintNameNoQuotes, tableNameWithoutSchema);
                        }

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
        /// Look for foreign key name conflicts, considering the 63 character limit on object names
        /// When conflicts are found, truncate to just before an underscore then add integers
        /// Also look for long foreign names and truncate them cleanly
        /// </summary>
        /// <param name="inputFile"></param>
        /// <param name="foreignKeyRenames">Dictionary where keys are table names and values are the foreign keys to rename for each table</param>
        /// <returns>True if successful, false if an error</returns>
        private bool ResolveForeignKeyNameConflicts(
            FileSystemInfo inputFile,
            out Dictionary<string, Dictionary<string, string>> foreignKeyRenames)
        {
            foreignKeyRenames = new Dictionary<string, Dictionary<string, string>>();

            var foreignKeysByTable = new Dictionary<string, List<string>>();

            try
            {
                var addConstraintMatcher = new Regex(
                    "^ *ALTER TABLE (?<TableName>.+?) ADD CONSTRAINT (?<ConstraintName>.+?) (?<ConstraintType>UNIQUE|PRIMARY KEY|FOREIGN KEY)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                // Cache the foreign key names
                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();

                    if (string.IsNullOrWhiteSpace(dataLine))
                    {
                        continue;
                    }

                    var constraintMatch = addConstraintMatcher.Match(dataLine);

                    if (!constraintMatch.Success)
                    {
                        continue;
                    }

                    var constraintType = constraintMatch.Groups["ConstraintType"].Value;

                    if (!constraintType.Equals("FOREIGN KEY"))
                    {
                        // For now, this method only examines foreign key constraints
                        continue;
                    }

                    var constraintName = constraintMatch.Groups["ConstraintName"].Value;
                    var constraintNameNoQuotes = TrimQuotes(constraintName);

                    var tableName = constraintMatch.Groups["TableName"].Value;
                    var tableNameWithoutSchema = TrimQuotes(GetNameWithoutSchema(tableName));

                    if (!foreignKeysByTable.ContainsKey(tableNameWithoutSchema))
                    {
                        foreignKeysByTable.Add(tableNameWithoutSchema, new List<string>());
                    }

                    var foreignKeyNames = foreignKeysByTable[tableNameWithoutSchema];

                    foreignKeyNames.Add(constraintNameNoQuotes);
                }

                // Keys in this dictionary are foreign keys for the current table, truncated to 63 characters
                // Values are the full key names that correspond to the truncated name
                var truncatedForeignKeyMap = new Dictionary<string, List<string>>();

                // Examine the foreign keys on each table, looking for conflicts after truncating to 63 characters
                // In addition, look for long names and truncate at a clean position
                foreach (var currentTable in foreignKeysByTable)
                {
                    var tableName = currentTable.Key;

                    truncatedForeignKeyMap.Clear();
                    foreach (var foreignKey in currentTable.Value)
                    {
                        // Assure that the foreign key name starts with "fk_"
                        var foreignKeyNameToUse = foreignKey.StartsWith("t_", StringComparison.OrdinalIgnoreCase)
                            ? "fk_" + foreignKey
                            : foreignKey;

                        var truncatedName = TruncateString(foreignKeyNameToUse, MAX_OBJECT_NAME_LENGTH);

                        if (!truncatedForeignKeyMap.ContainsKey(truncatedName))
                        {
                            truncatedForeignKeyMap.Add(truncatedName, new List<string>());
                        }

                        var fullNames = truncatedForeignKeyMap[truncatedName];

                        fullNames.Add(foreignKey);
                    }

                    var additionalTruncationRequired =
                        truncatedForeignKeyMap.Any(truncatedForeignKey => truncatedForeignKey.Value.Count > 1 &&
                                                                          truncatedForeignKey.Key.Length == MAX_OBJECT_NAME_LENGTH);

                    if (additionalTruncationRequired)
                    {
                        // Two or more foreign key names will be truncated to exactly 63 characters
                        // Truncate to 62 characters to allow for appending an integer

                        TruncateDictionaryKeys(truncatedForeignKeyMap, MAX_OBJECT_NAME_LENGTH - 1);
                    }

                    // Keys in this dictionary are the original foreign key name; values are the shortened name
                    var foreignKeyNameMap = ConstructForeignKeyMap(truncatedForeignKeyMap);

                    // New name conflicts may have been introduced after renaming things; check for this, and rename further if required
                    var updatedNames = new SortedSet<string>();
                    var duplicateNameFound = false;

                    foreach (var updatedName in foreignKeyNameMap.Values)
                    {
                        if (updatedNames.Contains(updatedName))
                        {
                            duplicateNameFound = true;
                            break;
                        }

                        updatedNames.Add(updatedName);
                    }

                    if (duplicateNameFound)
                    {
                        // Duplicate renamed foreign key found

                        // Keys in this dictionary are foreign keys for the current table, truncated to around 60 characters
                        // Values are the full key names that correspond to the truncated name
                        var truncatedForeignKeyMap2 = new Dictionary<string, List<string>>();

                        foreach (var item in foreignKeyNameMap)
                        {
                            if (truncatedForeignKeyMap2.TryGetValue(item.Value, out var originalForeignKeyNames))
                            {
                                originalForeignKeyNames.Add(item.Key);
                            }
                            else
                            {
                                truncatedForeignKeyMap2.Add(item.Value, new List<string> { item.Key });
                            }
                        }

                        var additionalTruncationRequired2 =
                            truncatedForeignKeyMap2.Any(truncatedForeignKey => truncatedForeignKey.Value.Count > 1 &&
                                                                               truncatedForeignKey.Key.Length == MAX_OBJECT_NAME_LENGTH);

                        if (additionalTruncationRequired2)
                        {
                            // Two or more foreign key names will be truncated to exactly 63 characters
                            // Truncate to 62 characters to allow for appending an integer

                            TruncateDictionaryKeys(truncatedForeignKeyMap2, MAX_OBJECT_NAME_LENGTH - 1);
                        }

                        // Keys in this dictionary are the original foreign key name; values are the shortened name
                        var foreignKeyNameMap2 = ConstructForeignKeyMap(truncatedForeignKeyMap2);

                        // Replace the contents of foreignKeyNameMap with foreignKeyNameMap2
                        foreignKeyNameMap.Clear();

                        foreach (var item in foreignKeyNameMap2)
                        {
                            foreignKeyNameMap.Add(item.Key, item.Value);
                        }
                    }

                    // Populate a dictionary with renamed foreign keys
                    var renamedForeignKeys = new Dictionary<string, string>();

                    // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var item in foreignKeyNameMap)
                    {
                        if (item.Key.Equals(item.Value))
                            continue;

                        renamedForeignKeys.Add(item.Key, item.Value);
                    }

                    if (renamedForeignKeys.Count == 0)
                        continue;

                    var newKeyNames = new StringBuilder();

                    foreach (var item in renamedForeignKeys.Values)
                    {
                        newKeyNames.AppendFormat("\n    {0}", item);
                    }

                    OnWarningEvent(
                        "Foreign key constraint{0} on table {1} {2} longer than {3} characters; truncating to: {4}",
                        renamedForeignKeys.Count > 1 ? "s" : string.Empty,
                        tableName,
                        renamedForeignKeys.Count > 1 ? "are" : "is",
                        MAX_OBJECT_NAME_LENGTH,
                        newKeyNames);

                    foreignKeyRenames.Add(tableName, renamedForeignKeys);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ResolveForeignKeyNameConflicts", ex);
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
        /// Dictionary where keys are new table names and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
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
            var referencedTables = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            var indexName = indexMatch.Groups["IndexName"].Value;

            // Determine the table for this index
            var tableNameWithSchema = indexMatch.Groups["TableNameWithSchema"].Value;

            var tableName = TrimQuotes(GetNameWithoutSchema(tableNameWithSchema));
            referencedTables.Add(tableName);

            var updatedLine = NameUpdater.UpdateColumnNames(columnNameMap, referencedTables, sourceDataLine, false, false);

            // Check for old column names in the index name

            foreach (var updatedTableName in referencedTables)
            {
                if (!columnNameMap.TryGetValue(updatedTableName, out var nameMapping))
                    continue;

                var tableNameIndex = indexName.IndexOf(tableName, StringComparison.OrdinalIgnoreCase);

                var indexNamePortion = tableNameIndex < 0
                    ? indexName
                    : indexName.Substring(tableNameIndex + tableName.Length);

                foreach (var item in nameMapping.Values)
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

                    indexName = tableNameIndex < 0
                        ? indexNamePortion
                        : indexName.Substring(0, tableNameIndex + tableName.Length) + indexNamePortion;
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

            if (indexNameNoQuotes.Length >= MAX_OBJECT_NAME_LENGTH)
            {
                // Need to shorten the name
                var truncatedName = TruncateString(indexNameNoQuotes, MAX_OBJECT_NAME_LENGTH);

                if (indexAndConstraintNames.ContainsKey(truncatedName))
                {
                    // Append an integer to generate a unique name
                    indexNameNoQuotes = GetUniqueName(truncatedName, tableName, indexAndConstraintNames, false);
                }
                else
                {
                    indexAndConstraintNames.Add(truncatedName, tableName);
                    indexNameNoQuotes = truncatedName;
                }

                OnWarningEvent(
                    "Index name on table {0} is longer than {1} characters; truncating to:\n    {2}",
                    tableName, MAX_OBJECT_NAME_LENGTH, indexNameNoQuotes);

                // Update indexName, surrounding with double quotes
                indexName = string.Format("\"{0}\"", indexNameNoQuotes);
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

        private void TruncateDictionaryKeys(Dictionary<string, List<string>> truncatedForeignKeyMap, int maximumLength)
        {
            var truncatedForeignKeyMapUpdates = new Dictionary<string, List<string>>();

            foreach (var truncatedForeignKey in truncatedForeignKeyMap)
            {
                var shortenedName = TruncateString(truncatedForeignKey.Key, maximumLength);
                truncatedForeignKeyMapUpdates.Add(shortenedName, truncatedForeignKey.Value);
            }

            truncatedForeignKeyMap.Clear();

            foreach (var replacement in truncatedForeignKeyMapUpdates)
            {
                truncatedForeignKeyMap.Add(replacement.Key, replacement.Value);
            }
        }

        /// <summary>
        /// Examine the object name's length and return a shortened string if too long
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="maximumLength"></param>
        /// <returns>
        /// String with length less than or equal to the specified maximum length, shortened to just before the last underscore
        /// </returns>
        private static string TruncateString(string objectName, int maximumLength)
        {
            if (objectName.Length <= maximumLength)
                return objectName;

            var trimmedName = objectName.Substring(0, maximumLength);

            if (trimmedName.EndsWith("_"))
                return trimmedName.TrimEnd('_');

            // Find the last underscore
            var lastUnderscore = trimmedName.LastIndexOf('_');

            return lastUnderscore < 1
                ? trimmedName
                : trimmedName.Substring(0, lastUnderscore);
        }
    }
}
