# Changelog

All notable changes to Sniffly will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2025-07-27

## Fixed
- Error analysis for notebooks

## [0.1.4] - 2025-07-21

## Added
- Windows support
- Instruction for running Sniffly on a remote reserver
- Removed redundant logs

## [0.1.0] - 2025-07-15

### Added
- Initial release of Sniffly - Claude Code Analytics Dashboard
- **Core Analytics Features**:
  - Message history browser with search and filtering
  - Token usage tracking with cost analysis
  - Tool usage statistics and patterns
  - Activity patterns by hour and day
  - Model usage breakdown across Claude versions
  - User interaction analysis with command complexity metrics
  - Error tracking and categorization
  - Project overview page showing all Claude projects
- **Performance Features**:
  - Two-tier caching system (memory LRU + disk JSON)
  - Processing speed of ~27,000 messages/second
  - Smart refresh with change detection
  - Pagination for large message sets
- **CLI Commands**:
  - `sniffly init` - Start dashboard server with auto-browser opening
  - `sniffly config` - View and manage configuration
  - `sniffly clear-cache` - Clear analytics cache
  - `sniffly version` - Show version information
  - `sniffly help` - Display comprehensive help
- **Share Functionality**:
  - Create shareable links of analytics dashboards
  - Optional inclusion of user commands
  - Public gallery for shared dashboards
  - Interactive charts in shared views
- **Admin Features**:
  - Google OAuth authentication
  - Feature/unfeature projects in gallery
  - Content moderation capabilities
- **Developer Experience**:
  - No external database required
  - Pure Python implementation (no Node.js dependency)
  - Comprehensive test suite with 80%+ coverage
  - GitHub Actions for CI/CD
  - Support for Python 3.10, 3.11, and 3.12

### Security
- All analytics processing happens locally on user's machine
- Sensitive file paths sanitized in shared dashboards
- Admin access restricted by email whitelist
- Share IDs use 24-character UUIDs for uniqueness

### Known Issues
- Overview page refresh button intermittently fails to detect project changes
  - Workaround: Refresh individual project dashboards first

[0.1.0]: https://github.com/chiphuyen/sniffly/releases/tag/v0.1.0