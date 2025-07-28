# Google Sheets Join - Architecture & Design Document

## Overview

Protocollie is a desktop application built with Tauri 2.0 that serves as a comprehensive testing and exploration tool for Model Context Protocol servers. Similar to Postman for APIs, it allows developers to connect to multiple MCP servers, explore their capabilities, and test their functionality in real-time.

## Target Users

Google Sheets Join is a Google Spreadsheets extension that allows user to join arbitrary ranges of cells together, creating a new range that can be used in formulas. It is designed to work with Google Sheets and provides a user-friendly interface for managing cell ranges. It is intended for developers and data analysts who work with Google Sheets and need to manipulate cell ranges efficiently. It will try to stay as close as possible to the expected behavior from a SQL JOIN operation, while being performant.

## Development Phases

### Phase 1: Core MVP ✅ (41/41 stories completed)
- JOIN command that enable the user to provide 2 data ranges and 2 columns index to perform a Nested‑Loop Join.
  e.g. `=JOIN(A1:A10, B1:B10, 1, 2)` will join the first column of range A with the second column of range B.

    The simplest: for each row in table A, scan table B (or index‑lookup in B) for matching rows.

    Why it can be fast: if one side is very small (or you’ve already filtered it down via a WHERE), the inner loop is tiny; if there’s an index on the join column, each lookup is O(log N) instead of O(N).


