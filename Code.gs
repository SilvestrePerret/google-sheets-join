/**
 * Joins two ranges based on matching values in specified columns
 * @param {A1:C20} left_range First range to join, e.g. A1:C20
 * @param {E1:H4} right_range Second range to join, e.g. E1:H4
 * @param {1} left_columns Column index(es) in left_range to match on (1-based), e.g. 1 or {1, 2}
 * @param {2} right_columns Column index(es) in right_range to match on (1-based), e.g. 1 or {1, 2}
 * @param {"INNER"} join_type [OPTIONAL] Type of join (INNER, LEFT). Defaults to INNER if not specified
 * @param {TRUE} has_header [OPTIONAL] Whether the ranges have a header row. Defaults to true
 * @returns {Array} Combined range with matched rows
 * @customfunction
 */
function SQLJOIN(left_range, right_range, left_columns, right_columns, join_type, has_header) {
  // 0. Handle default run case 
  if (left_range === undefined && right_range === undefined && left_columns === undefined && right_columns === undefined && join_type === undefined && has_header === undefined)
    return null;

  // 1. Validate and normalize join_type parameter
  var normalizedJoinType = validateAndNormalizeJoinType(join_type);

  // 2. Validate and normalize column indexes
  var normalizedColumns = validateAndNormalizeColumnIndexes(left_columns, right_columns);

  // 3. Validate and normalize range parameters
  var normalizedRanges = validateAndNormalizeRanges(
    left_range,
    right_range,
    normalizedColumns.col1Indexes,
    normalizedColumns.col2Indexes,
    has_header
  );

  return performHashedJoin(
    normalizedRanges.left_range,
    normalizedRanges.right_range,
    normalizedColumns.col1ZeroIndexes,
    normalizedColumns.col2ZeroIndexes,
    normalizedJoinType,
    has_header || true);
}

/**
 * Validates and normalizes the join type parameter
 * @param {string} join_type - Type of join (INNER, LEFT)
 * @returns {string} Normalized join type in uppercase
 * @throws {Error} If join type is not supported
 */
function validateAndNormalizeJoinType(join_type) {
  join_type = join_type || 'INNER';
  join_type = join_type.toUpperCase();

  if (join_type !== 'INNER' && join_type !== 'LEFT') {
    throw new Error(`Unsupported join_type parameters. Expected: 'INNER', 'LEFT'; Got: '${join_type}'`);
  }

  return join_type;
}

/**
 * Validates and normalizes column index parameters
 * @param {number|Array} left_columns - Column index(es) for left range
 * @param {number|Array} right_columns - Column index(es) for right range
 * @returns {Object} Object containing validated 1-based and 0-based column indexes
 * @throws {Error} If column indexes are invalid
 */
function validateAndNormalizeColumnIndexes(left_columns, right_columns) {
  // Convert single indexes to arrays
  var col1Indexes = Array.isArray(left_columns) ? left_columns[0] : [left_columns];
  var col2Indexes = Array.isArray(right_columns) ? right_columns[0] : [right_columns];

  // Validate that both arrays have the same length
  if (col1Indexes.length !== col2Indexes.length) {
    throw new Error(`left_columns and right_columns parameters must contain the same number of elements, Got: left_columns=${col1Indexes}, right_columns=${col2Indexes}`);
  }

  // Check for empty column arrays
  if (col1Indexes.length === 0 || col2Indexes.length === 0) {
    throw new Error("Column indexes cannot be empty. You must specify at least one column to join on.");
  }

  // Validate types and check for duplicates
  var col1Set = new Set();
  var col2Set = new Set();

  for (var i = 0; i < col1Indexes.length; i++) {
    if (!Number.isInteger(col1Indexes[i])) {
      throw new Error(`left_columns contains non integer value(s): Got '${col1Indexes[i]}' at the ${i} position`)
    }
    if (!Number.isInteger(col2Indexes[i])) {
      throw new Error(`right_columns contains non integer value(s): Got '${col2Indexes[i]}' at the ${i} position`)
    }

    // Check for duplicate columns in join keys
    if (col1Set.has(col1Indexes[i])) {
      throw new Error(`left_columns contains duplicate column index: ${col1Indexes[i]}`);
    }
    if (col2Set.has(col2Indexes[i])) {
      throw new Error(`right_columns contains duplicate column index: ${col2Indexes[i]}`);
    }

    col1Set.add(col1Indexes[i]);
    col2Set.add(col2Indexes[i]);
  }

  // Check for minimum column bounds (should be at least 1)
  if (Math.min(...col1Indexes) < 1 || Math.min(...col2Indexes) < 1) {
    throw new Error("Column indexes must be positive integers starting from 1 (1-based indexing).");
  }

  // Convert to 0-based indexes
  var col1ZeroIndexes = col1Indexes.map(idx => idx - 1);
  var col2ZeroIndexes = col2Indexes.map(idx => idx - 1);

  return {
    col1Indexes: col1Indexes,
    col2Indexes: col2Indexes,
    col1ZeroIndexes: col1ZeroIndexes,
    col2ZeroIndexes: col2ZeroIndexes
  };
}

/**
 * Validates and normalizes range parameters
 * @param {Array} left_range - First range to join
 * @param {Array} right_range - Second range to join
 * @param {Array} col1Indexes - Column indexes for left range (1-based)
 * @param {Array} col2Indexes - Column indexes for right range (1-based)
 * @param {boolean} has_header - Whether ranges have header rows
 * @returns {Object} Object containing validated and truncated ranges
 * @throws {Error} If ranges are invalid
 */
function validateAndNormalizeRanges(left_range, right_range, col1Indexes, col2Indexes, has_header) {
  // Basic type validation
  if (!Array.isArray(left_range) || !Array.isArray(right_range)) {
    throw new Error("left_range and right_range parameters MUST be ranges.")
  }

  if (left_range.length === 0 || right_range.length === 0) {
    throw new Error("left_range and right_range parameters MUST be non-empty ranges. Check your formula")
  }

  // Truncate empty rows and columns from both ranges
  left_range = truncateEmptyRowsAndColumns(left_range);
  right_range = truncateEmptyRowsAndColumns(right_range);

  // Check that ranges are not completely empty after truncation
  if (left_range.length === 0 || left_range[0].length === 0) {
    throw new Error("left_range contains only empty values after removing trailing empty rows/columns.");
  }

  if (right_range.length === 0 || right_range[0].length === 0) {
    throw new Error("right_range contains only empty values after removing trailing empty rows/columns.");
  }

  // Check that all rows in each range have same number of columns
  var row1Length = left_range[0].length;
  var row2Length = right_range[0].length;

  var isLeftRangeValid = left_range.every(row => row.length === row1Length);
  var isRightRangeValid = right_range.every(row => row.length === row2Length);

  if (!isLeftRangeValid || !isRightRangeValid) {
    throw new Error("All rows in each range must have the same number of columns. Check for irregular data in your ranges.");
  }

  // Validate column indexes are within bounds
  if (Math.max(...col1Indexes) > row1Length || Math.max(...col2Indexes) > row2Length) {
    throw new Error(`Column indexes are out of bounds. left_range has ${row1Length} columns, right_range has ${row2Length} columns. Check left_columns and right_columns parameters.`);
  }

  // Check that ranges have enough rows (at least header if has_header is true)
  var minRows = (has_header || true) ? 1 : 0;
  if (left_range.length <= minRows || right_range.length <= minRows) {
    throw new Error(`Ranges must contain data rows${(has_header || true) ? ' in addition to the header row' : ''}. One or both ranges only contain header/empty data.`);
  }

  return {
    left_range: left_range,
    right_range: right_range
  };
}

/**
 * Creates a composite key from multiple column values for hash table lookup
 * @param {Array} row - Data row
 * @param {Array} columnIndexes - Array of column indexes to include in key
 * @returns {string} Composite key string
 */
function createCompositeKey(row, columnIndexes) {
  return columnIndexes
    .map(idx => String(row[idx]))
    .join('\u001E');
}

/**
 * Performs a hash-based join operation between two data arrays for better performance
 * @param {Array} data1 - First data array
 * @param {Array} data2 - Second data array
 * @param {Array} col1ZeroIndexes - Column indexes for data1 (0-based)
 * @param {Array} col2ZeroIndexes - Column indexes for data2 (0-based)
 * @param {string} joinType - Type of join (INNER, LEFT)
 * @param {boolean} hasHeader - Whether the data has header rows
 * @returns {Array} Array of joined rows
 */
function performHashedJoin(data1, data2, col1ZeroIndexes, col2ZeroIndexes, joinType, hasHeader) {
  var result = [];
  var data2Columns = data2.length > 0 ? data2[0].length : 0;
  var filteredData2Columns = data2Columns - col2ZeroIndexes.length;

  // Handle header row
  if (hasHeader && data1.length > 0 && data2.length > 0) {
    var headerRow1 = data1[0];
    var headerRow2 = data2[0].filter((_, index) => !col2ZeroIndexes.includes(index));
    result.push(headerRow1.concat(headerRow2));
  }

  // Determine start index based on header presence
  var startIndex = hasHeader ? 1 : 0;

  // Build hash table from data2
  var hashTable = {};

  for (var j = startIndex; j < data2.length; j++) {
    var row2 = data2[j];
    var key = createCompositeKey(row2, col2ZeroIndexes);

    // Initialize array if key doesn't exist
    if (!hashTable[key]) {
      hashTable[key] = [];
    }

    // Store filtered row (without join columns)
    var filteredRow2 = row2.filter((_, index) => !col2ZeroIndexes.includes(index));

    hashTable[key].push(filteredRow2);
  }

  // Process data1 and lookup matches in hash table
  for (var i = startIndex; i < data1.length; i++) {
    var row1 = data1[i];
    var key = createCompositeKey(row1, col1ZeroIndexes);
    var matches = hashTable[key];

    if (matches && matches.length > 0) {
      // Add all matching rows
      for (var m = 0; m < matches.length; m++) {
        var combinedRow = row1.concat(matches[m]);
        result.push(combinedRow);
      }
    } else if (joinType === 'LEFT') {
      // Handle unmatched rows for LEFT join
      var nullValues = new Array(filteredData2Columns).fill(null);
      var combinedRow = row1.concat(nullValues);
      result.push(combinedRow);
    }
  }

  return result;
}

/**
 * Finds the last row index that contains at least one non-empty value
 * @param {Array} range - 2D array representing the range
 * @returns {number} Last non-empty row index, or -1 if all rows are empty
 */
function findLastNonEmptyRow(range) {
  return range.findLastIndex(row => row.some(cell => cell !== ''));
}

/**
 * Finds the last column index that contains at least one non-empty value
 * @param {Array} range - 2D array representing the range
 * @returns {number} Last non-empty column index, or -1 if all columns are empty
 */
function findLastNonEmptyColumn(range) {
  if (range.length === 0) return -1;

  var maxColIndex = range[0].length - 1;

  for (var colIdx = maxColIndex; colIdx >= 0; colIdx--) {
    var hasNonEmpty = range.some(row => row[colIdx] !== '');
    if (hasNonEmpty) {
      return colIdx;
    }
  }

  return -1;
}

/**
 * Truncates a range by removing trailing empty rows and columns
 * @param {Array} range - 2D array representing the range
 * @returns {Array} Truncated range
 */
function truncateEmptyRowsAndColumns(range) {
  if (!range || range.length === 0) return range;

  var lastRowIdx = findLastNonEmptyRow(range);
  if (lastRowIdx === -1) return [[]];


  var lastRowIdx = findLastNonEmptyRow(range);
  if (lastRowIdx === -1) return [[]];
  var truncatedRows = range.slice(0, lastRowIdx + 1);
  var truncatedRows = range.slice(0, lastRowIdx + 1);


  var truncatedRows = range.slice(0, lastRowIdx + 1);

  var lastColIdx = findLastNonEmptyColumn(truncatedRows);
  if (lastColIdx === -1) return [[]];

  return truncatedRows.map(row => row.slice(0, lastColIdx + 1));
}
