# Operational Documentation

This folder contains historical documentation about operations, bug fixes, 
and feature implementations. These documents are generated during development
and provide context for future reference but are not part of the main 
user-facing documentation.

## About This Folder

- Documents are organized by date: `YYYY-MM-DD-brief-description/`
- Subdirectories are gitignored (not checked into version control)
- Only this README is version controlled
- Useful for understanding "why" decisions were made

## Index

### 2025-11-29 - Test Infrastructure Improvements

**Location**: `2025-11-29-test-improvements/` (gitignored)

**Summary**: Comprehensive enhancements to testing infrastructure including:
- Integration test improvements to verify actual database changes
- 27 new unit tests for DDL regenerator (reviewed DDL workflow)
- 10 tests for BINARY/VARIANT type support
- Test runner script (`run_tests.sh`) to handle import conflicts
- Total: 244 unit tests now passing

**Key Files**:
- `IMPLEMENTATION_COMPLETE.md` - Complete feature summary
- `TEST_IMPROVEMENTS_SUMMARY.md` - Detailed test enhancements
- `PLAN_EXECUTION_SUMMARY.md` - Task-by-task completion
- `BINARY_VARIANT_SUPPORT.md` - Technical details on type conversion

**Key Achievements**:
- Fixed `apply_ddl=false` bug
- Added BINARY → base64 and VARIANT → JSON conversion
- Enhanced integration tests to verify DB changes (not just logs)
- Created comprehensive DDL regenerator unit tests

---

## Guidelines

**When adding new operational docs**:

1. Create dated folder: `YYYY-MM-DD-brief-name/`
2. Move all summary/implementation docs there
3. Update this README's index section
4. Commit only the README update, not the docs themselves

**What belongs here**:
- Implementation summaries
- Bug fix explanations
- Session transcripts
- Architecture decision records
- Files with SUMMARY/COMPLETE/EXECUTION in the name

**What does NOT belong here**:
- User-facing documentation (goes in main README or docs/user-guides/)
- API documentation
- Setup instructions
- Test runner docs that users need (keep at root or in docs/)

