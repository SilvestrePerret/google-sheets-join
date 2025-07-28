# Google Sheets JOIN - Phase 1 Project Plan

## Overview
Phase 1 is the MVP!

## Story Status Legend
- ⏳ **Pending** - Not started
- 🔄 **In Progress** - Currently being worked on  
- ✅ **Completed** - Implemented and verified
- 🧪 **Testing** - Implementation complete, awaiting verification

---

## Phase 1.0: Core MVP

### Story 1.0.1: Implement naive JOIN function
**Status:** ✅ **Completed**

**As a developer, I want a naive JOIN function so that I can combine data from two ranges.**

**Acceptance Criteria:**
- Implement a basic JOIN function that takes two ranges and returns a combined range.
- Function signature: `=JOIN(range1, range2, column_index1, column_index2)`
- The function should return a new range that combines the two input ranges based on the specified column indices.
- The function should handle cases where the ranges have different sizes.
- The function should return an empty range if no matches are found.
- The function should be implement a naive nested-loop join algorithm.


### Story 1.0.2: Add support for JOIN with multiple columns
**Status:** ✅ **Completed**

**As a developer, I want to perform more complex joins.**

**Acceptance Criteria:**
- Extend the JOIN function to support joining on multiple columns.
- Function signature: `=JOIN(range1, range2, column_index1, column_index2)`
- The function should allow specifying multiple column indices for both ranges.
- The function should return a new range that combines the two input ranges based on the specified column indices.
- The function should handle cases where the ranges have different sizes.
- The function should return an empty range if no matches are found.

### Story 1.0.3: Add support for LEFT, RIGHT, and FULL JOIN
**Status:** ⏳ **Pending**

**As a developer, I want to perform different types of joins.**

**Acceptance Criteria:**
- Extend the JOIN function to support LEFT, RIGHT, and FULL joins.
- Function signature: `=JOIN(range1, range2, column_index1, column_index2, join_type)`
- The function should allow specifying the type of join (LEFT, RIGHT, FULL). If not specified, it defaults to INNER JOIN.
- The function should return a new range that combines the two input ranges based on the specified join type.

