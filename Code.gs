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
  var params = validateJoinParameters(left_range, right_range, left_columns, right_columns, join_type, has_header);

  if (!params) {
    return [];
  }

  return performHashedJoin(params.data1, params.data2, params.col1ZeroIndexes, params.col2ZeroIndexes, params.joinType, params.hasHeader);
}

/**
 * Validates and normalizes the input parameters for SQLJOIN
 * @param {Range|Array} left_range - First range to join
 * @param {Range|Array} right_range - Second range to join
 * @param {number|Array} left_columns - Column index(es) in left_range to match on (1-based)
 * @param {number|Array} right_columns - Column index(es) in right_range to match on (1-based)
 * @param {string} join_type - Type of join (INNER, LEFT). Defaults to INNER if not specified
 * @param {boolean} has_header - Whether the ranges have header rows. Defaults to true
 * @returns {Object|null} Normalized parameters object or null if validation fails
 */
function validateJoinParameters(left_range, right_range, left_columns, right_columns, join_type, has_header) {
  if (!left_range || !right_range || !left_columns || !right_columns) {
    return null;
  }

  // Validate join_type parameter
  join_type = join_type || 'INNER';
  join_type = join_type.toUpperCase();
  if (join_type !== 'INNER' && join_type !== 'LEFT') {
    return null;
  }

  // Convert single indexes to arrays for uniform handling
  var col1Indexes = Array.isArray(left_columns) ? left_columns[0] : [left_columns];
  var col2Indexes = Array.isArray(right_columns) ? right_columns[0] : [right_columns];

  // Validate that both arrays have the same length
  if (col1Indexes.length !== col2Indexes.length) {
    return null;
  }

  if (typeof left_range.getValues === 'function') {
    data1 = left_range.getValues();
  }
  else {
    data1 = left_range;
  }


  if (typeof right_range.getValues === 'function') {
    data2 = right_range.getValues();
  }
  else {
    data2 = leright_rangeft_range;
  }

  if (!Array.isArray(data1) || !Array.isArray(data2)) {
    return null;
  }

  if (data1.length === 0 || data2.length === 0) {
    return null;
  }

  // Check that all rows in each range have same number of columns
  var row1Length = data1[0].length;
  var row2Length = data2[0].length;

  for (var i = 0; i < data1.length; i++) {
    if (data1[i].length !== row1Length) {
      return null;
    }
  }

  for (var i = 0; i < data2.length; i++) {
    if (data2[i].length !== row2Length) {
      return null;
    }
  }

  // Validate column indexes are within bounds
  if (Math.max(...col1Indexes) > row1Length || Math.max(...col2Indexes) > row2Length) {
    return null;
  }


  // Convert to 0-based indexes and validate
  var col1ZeroIndexes = [];
  var col2ZeroIndexes = [];

  for (var k = 0; k < col1Indexes.length; k++) {
    var idx1 = col1Indexes[k] - 1;
    var idx2 = col2Indexes[k] - 1;

    if (idx1 < 0 || idx2 < 0) {
      return null;
    }

    col1ZeroIndexes.push(idx1);
    col2ZeroIndexes.push(idx2);
  }

  return {
    data1: data1,
    data2: data2,
    col1ZeroIndexes: col1ZeroIndexes,
    col2ZeroIndexes: col2ZeroIndexes,
    joinType: join_type,
    hasHeader: has_header || true
  };
}

/**
 * Creates a composite key from multiple column values for hash table lookup
 * @param {Array} row - Data row
 * @param {Array} columnIndexes - Array of column indexes to include in key
 * @returns {string} Composite key string
 */
function createCompositeKey(row, columnIndexes) {
  var keyParts = [];
  for (var i = 0; i < columnIndexes.length; i++) {
    var value = row[columnIndexes[i]];
    // Convert to string and escape delimiter to handle edge cases
    keyParts.push(String(value));
  }
  return keyParts.join('\u001E');
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
    var headerRow2 = data2[0].filter(function (_, index) {
      return col2ZeroIndexes.indexOf(index) === -1;
    });
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

    // Store filtered row (without join columns) and original index
    var filteredRow2 = row2.filter(function (_, index) {
      return col2ZeroIndexes.indexOf(index) === -1;
    });

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
      var nullValues = [];
      for (var n = 0; n < filteredData2Columns; n++) {
        nullValues.push(null);
      }
      var combinedRow = row1.concat(nullValues);
      result.push(combinedRow);
    }
  }

  return result;
}
