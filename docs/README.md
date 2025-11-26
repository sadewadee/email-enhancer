# Documentation Index

## Overview

This directory contains comprehensive documentation for the Email Scraper & Validator system. Each document covers specific aspects of the system, from basic usage to advanced configurations and troubleshooting.

## Documentation Structure

```
docs/
├── README.md                    # This file - documentation index
├── instream_outstream_csv.md  # CSV streaming and dynamic file handling
├── multi_project_clone.md      # GitPython multi-project cloning guide
├── cli_flags_reference.md      # Complete CLI flags reference
└── [future documentation files] # To be added as needed
```

## Available Documentation

### 📊 [CSV Instream/Outstream Documentation](instream_outstream_csv.md)

**Purpose:** Comprehensive guide to CSV file streaming, dynamic growth handling, and real-time processing capabilities.

**Key Topics:**
- Dynamic file growth detection (japan.csv 500KB → 1MB scenario)
- Chunked reading for large datasets
- Memory-efficient processing strategies
- Real-time CSV writing with atomic operations
- Completion markers and error recovery

**Use Cases:**
- Real-time data pipelines where input files grow during processing
- Large dataset processing with memory constraints
- Resume capability for interrupted processing
- Production monitoring and status tracking

### 🚀 [Multi-Project Clone Documentation](multi_project_clone.md)

**Purpose:** Step-by-step guide for cloning one Git repository into multiple independent projects using GitPython and sparse checkout.

**Key Topics:**
- Sparse checkout optimization for bandwidth efficiency
- Independent Git histories for each project
- Project-specific configurations and post-clone commands
- Automated setup and initialization scripts
- CI/CD integration examples

**Use Cases:**
- Monorepo management with project variations
- Template-based project generation
- Independent development workflows
- Automated deployment pipelines

### ⚙️ [CLI Flags Reference](cli_flags_reference.md)

**Purpose:** Complete reference of all command-line flags, their purposes, and usage examples.

**Key Topics:**
- Subcommands (single, batch, url)
- Performance and concurrency flags
- Resource management options
- Cloudflare and anti-bot configurations
- Proxy and retry management
- Output format and logging controls

**Use Cases:**
- Performance tuning for different environments
- Memory-constrained system configurations
- Development vs production setups
- Troubleshooting and debugging scenarios

## Quick Start Guides

### For New Users

1. **Read the CSV Streaming Guide** - Understand how to process growing files
2. **Check CLI Flags Reference** - Find optimal flags for your use case
3. **Use Multi-Project Clone** - Set up development environments efficiently

### For Advanced Users

1. **Combine Features** - Use streaming with multi-project workflows
2. **Optimize Performance** - Fine-tune flags for your hardware
3. **Automate Workflows** - Integrate with CI/CD pipelines

## Navigation Tips

### By Use Case

**I want to process large CSV files:**
- Start with [CSV Instream/Outstream Documentation](instream_outstream_csv.md)
- Review chunking and memory management sections
- Check performance flags in [CLI Flags Reference](cli_flags_reference.md)

**I need to manage multiple projects:**
- Go to [Multi-Project Clone Documentation](multi_project_clone.md)
- Follow the step-by-step GitPython examples
- Review automation and CI/CD integration sections

**I want to optimize performance:**
- Review [CLI Flags Reference](cli_flags_reference.md) performance section
- Check resource management flags
- Understand worker and timeout configurations

**I'm troubleshooting issues:**
- Check relevant sections in each documentation file
- Review error handling and recovery procedures
- Use debugging flags and logging options

### By Documentation Type

**Configuration & Setup:**
- [CLI Flags Reference](cli_flags_reference.md) - All command-line options
- [Multi-Project Clone Documentation](multi_project_clone.md) - Project setup

**Data Processing:**
- [CSV Instream/Outstream Documentation](instream_outstream_csv.md) - File handling and streaming

**Advanced Features:**
- [Multi-Project Clone Documentation](multi_project_clone.md) - Git automation
- [CLI Flags Reference](cli_flags_reference.md) - Advanced configurations

## Integration Examples

### Complete Workflow Example

```bash
# 1. Set up multiple projects from a monorepo
python docs/../clone_to_multiple_projects.py \
  --repo https://github.com/company/monorepo.git \
  --config docs/../projects.json \
  --force

# 2. Process growing CSV file with optimal settings
python main.py single projects/web-app/data.csv \
  --output-dir results/ \
  --workers 10 \
  --chunk-size 1000 \
  --light-load \
  --cf-wait-timeout 90 \
  --report

# 3. Monitor processing in real-time
tail -f logs/*.log
```

### Development Environment Setup

```bash
# 1. Clone development project
python clone_to_multiple_projects.py \
  --repo https://github.com/company/shared-components.git \
  --projects dev:/home/user/dev-project \
  --shared-paths core/,templates/ \
  --force

# 2. Navigate and setup
cd /home/user/dev-project
npm install
npm run dev

# 3. Process test data
python /path/to/email-scraper/main.py single test-data.csv \
  --limit 100 \
  --log-level DEBUG
```

## Contributing to Documentation

### Adding New Documentation

1. **Create new file** in `docs/` directory with descriptive name
2. **Follow existing format** - Use consistent structure and styling
3. **Update this index** - Add entry with brief description
4. **Cross-reference** - Link between related documentation files

### Documentation Standards

- **Clear examples** - Provide copy-paste ready code blocks
- **Use case focus** - Address specific user scenarios
- **Troubleshooting** - Include common issues and solutions
- **Performance notes** - Address resource usage and optimization

## File Relationships

```
Root Directory/
├── main.py                    # Main application (uses flags from cli_flags_reference.md)
├── csv_processor.py           # CSV processing (documented in instream_outstream_csv.md)
├── run.sh                     # Execution wrapper
├── installer.sh                # Environment setup
└── docs/                      # This documentation directory
    ├── README.md                # This index file
    ├── instream_outstream_csv.md
    ├── multi_project_clone.md
    └── cli_flags_reference.md
```

## Getting Help

### Command Line Help
```bash
# Main help
python main.py --help

# Subcommand help
python main.py single --help
python main.py batch --help
python main.py url --help
```

### Documentation Access
```bash
# Open documentation in browser (if supported)
open docs/README.md

# View specific documentation
cat docs/cli_flags_reference.md
less docs/instream_outstream_csv.md
```

## Version Information

Documentation version: 1.0.0  
Last updated: 2025-11-25  
Compatible with Email Scraper & Validator v0.1.x

## Feedback and Support

### Documentation Issues
- **Clarity problems** - Report unclear sections or examples
- **Missing information** - Report gaps in documentation
- **Outdated content** - Report inconsistencies with actual behavior

### Feature Requests
- **New documentation needs** - Suggest additional documentation topics
- **Format improvements** - Recommend better organization or presentation
- **Example additions** - Provide useful examples for edge cases

### Contact Channels
- **Technical issues** - Use main application issue reporting
- **Documentation feedback** - Create documentation-specific issues
- **General questions** - Refer to main project communication channels

---

**Tip:** Start with the specific documentation that matches your use case, then explore related topics for comprehensive understanding of the Email Scraper & Validator system.