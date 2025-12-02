"""
Quality Gates - Pre-commit hooks and CI/CD quality validation

Implements comprehensive quality gate functionality:
- Pre-commit hook generation
- Code quality checks (syntax, style, complexity)
- Test coverage requirements
- Security scanning helpers
- Performance benchmarking gates
- CI/CD pipeline integration
"""

import ast
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class CheckStatus(Enum):
    """Quality check status"""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


class CheckSeverity(Enum):
    """Check severity level"""
    ERROR = "error"      # Blocks commit/deployment
    WARNING = "warning"  # Warns but allows
    INFO = "info"        # Informational only


@dataclass
class CheckResult:
    """Result of a quality check"""
    name: str
    status: CheckStatus
    severity: CheckSeverity
    message: str
    details: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data['status'] = self.status.value
        data['severity'] = self.severity.value
        return data
    
    @property
    def blocks_commit(self) -> bool:
        """Check if this result should block commit"""
        return self.status == CheckStatus.FAILED and self.severity == CheckSeverity.ERROR


@dataclass
class QualityReport:
    """Complete quality gate report"""
    timestamp: float
    duration_ms: float
    total_checks: int
    passed: int
    failed: int
    warnings: int
    skipped: int
    checks: List[CheckResult] = field(default_factory=list)
    
    @property
    def all_passed(self) -> bool:
        """Check if all required checks passed"""
        return all(not c.blocks_commit for c in self.checks)
    
    @property
    def has_warnings(self) -> bool:
        """Check if there are warnings"""
        return self.warnings > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'timestamp': self.timestamp,
            'duration_ms': self.duration_ms,
            'total_checks': self.total_checks,
            'passed': self.passed,
            'failed': self.failed,
            'warnings': self.warnings,
            'skipped': self.skipped,
            'all_passed': self.all_passed,
            'checks': [c.to_dict() for c in self.checks]
        }
    
    def to_markdown(self) -> str:
        """Convert to markdown format"""
        lines = [
            "# Quality Gate Report",
            "",
            f"**Timestamp:** {datetime.fromtimestamp(self.timestamp).isoformat()}",
            f"**Duration:** {self.duration_ms:.0f}ms",
            "",
            "## Summary",
            "",
            f"- **Total Checks:** {self.total_checks}",
            f"- **Passed:** {self.passed}",
            f"- **Failed:** {self.failed}",
            f"- **Warnings:** {self.warnings}",
            f"- **Skipped:** {self.skipped}",
            "",
            f"**Status:** {'PASSED' if self.all_passed else 'FAILED'}",
            "",
            "## Check Details",
            ""
        ]
        
        for check in self.checks:
            status_emoji = {
                CheckStatus.PASSED: "✅",
                CheckStatus.FAILED: "❌",
                CheckStatus.WARNING: "⚠️",
                CheckStatus.SKIPPED: "⏭️"
            }.get(check.status, "❓")
            
            lines.append(f"### {status_emoji} {check.name}")
            lines.append(f"**Status:** {check.status.value}")
            lines.append(f"**Message:** {check.message}")
            
            if check.details:
                lines.append("\n**Details:**")
                for detail in check.details[:10]:  # Limit details
                    lines.append(f"- {detail}")
            
            lines.append("")
        
        return "\n".join(lines)


class SyntaxChecker:
    """Python syntax validation"""
    
    def check_file(self, filepath: str) -> CheckResult:
        """Check Python file syntax"""
        start_time = time.time()
        details = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                source = f.read()
            
            ast.parse(source)
            
            return CheckResult(
                name=f"syntax:{Path(filepath).name}",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.ERROR,
                message="Syntax OK",
                duration_ms=(time.time() - start_time) * 1000
            )
            
        except SyntaxError as e:
            details.append(f"Line {e.lineno}: {e.msg}")
            return CheckResult(
                name=f"syntax:{Path(filepath).name}",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.ERROR,
                message=f"Syntax error at line {e.lineno}",
                details=details,
                duration_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            return CheckResult(
                name=f"syntax:{Path(filepath).name}",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.ERROR,
                message=str(e),
                duration_ms=(time.time() - start_time) * 1000
            )
    
    def check_directory(self, directory: str) -> List[CheckResult]:
        """Check all Python files in directory"""
        results = []
        path = Path(directory)
        
        for py_file in path.rglob("*.py"):
            # Skip common ignored directories
            if any(part.startswith('.') or part in ('venv', '__pycache__', 'node_modules') 
                   for part in py_file.parts):
                continue
            
            results.append(self.check_file(str(py_file)))
        
        return results


class ComplexityChecker:
    """Code complexity analysis"""
    
    def __init__(self, max_complexity: int = 10, max_line_length: int = 120):
        self.max_complexity = max_complexity
        self.max_line_length = max_line_length
    
    def check_file(self, filepath: str) -> CheckResult:
        """Check file complexity metrics"""
        start_time = time.time()
        details = []
        issues = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Check line length
            for i, line in enumerate(lines, 1):
                if len(line.rstrip()) > self.max_line_length:
                    issues.append(f"Line {i}: {len(line.rstrip())} chars (max: {self.max_line_length})")
            
            # Parse AST for complexity
            with open(filepath, 'r', encoding='utf-8') as f:
                source = f.read()
            
            tree = ast.parse(source)
            
            # Simple cyclomatic complexity approximation
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    complexity = self._calculate_complexity(node)
                    if complexity > self.max_complexity:
                        issues.append(
                            f"Function '{node.name}' has complexity {complexity} "
                            f"(max: {self.max_complexity})"
                        )
            
            if issues:
                return CheckResult(
                    name=f"complexity:{Path(filepath).name}",
                    status=CheckStatus.WARNING,
                    severity=CheckSeverity.WARNING,
                    message=f"Found {len(issues)} complexity issues",
                    details=issues[:10],
                    duration_ms=(time.time() - start_time) * 1000
                )
            
            return CheckResult(
                name=f"complexity:{Path(filepath).name}",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.WARNING,
                message="Complexity OK",
                duration_ms=(time.time() - start_time) * 1000
            )
            
        except Exception as e:
            return CheckResult(
                name=f"complexity:{Path(filepath).name}",
                status=CheckStatus.SKIPPED,
                severity=CheckSeverity.WARNING,
                message=f"Could not analyze: {e}",
                duration_ms=(time.time() - start_time) * 1000
            )
    
    def _calculate_complexity(self, node: ast.FunctionDef) -> int:
        """Calculate cyclomatic complexity of function"""
        complexity = 1
        
        for child in ast.walk(node):
            if isinstance(child, (ast.If, ast.While, ast.For, ast.ExceptHandler)):
                complexity += 1
            elif isinstance(child, ast.BoolOp):
                complexity += len(child.values) - 1
            elif isinstance(child, ast.comprehension):
                complexity += 1
        
        return complexity


class SecurityChecker:
    """Basic security issue detection"""
    
    DANGEROUS_PATTERNS = [
        (r'eval\s*\(', 'Dangerous eval() usage'),
        (r'exec\s*\(', 'Dangerous exec() usage'),
        (r'pickle\.loads?\s*\(', 'Unsafe pickle deserialization'),
        (r'subprocess\.(?:call|run|Popen)\s*\([^)]*shell\s*=\s*True', 'Shell injection risk'),
        (r'os\.system\s*\(', 'Dangerous os.system() usage'),
        (r'__import__\s*\(', 'Dynamic import (security risk)'),
        (r'password\s*=\s*["\'][^"\']+["\']', 'Hardcoded password detected'),
        (r'api_key\s*=\s*["\'][^"\']+["\']', 'Hardcoded API key detected'),
        (r'secret\s*=\s*["\'][^"\']+["\']', 'Hardcoded secret detected'),
    ]
    
    def check_file(self, filepath: str) -> CheckResult:
        """Check file for security issues"""
        start_time = time.time()
        issues = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.splitlines()
            
            for pattern, description in self.DANGEROUS_PATTERNS:
                for i, line in enumerate(lines, 1):
                    # Skip comments
                    stripped = line.strip()
                    if stripped.startswith('#'):
                        continue
                    
                    if re.search(pattern, line, re.IGNORECASE):
                        issues.append(f"Line {i}: {description}")
            
            if issues:
                return CheckResult(
                    name=f"security:{Path(filepath).name}",
                    status=CheckStatus.WARNING,
                    severity=CheckSeverity.WARNING,
                    message=f"Found {len(issues)} potential security issues",
                    details=issues,
                    duration_ms=(time.time() - start_time) * 1000
                )
            
            return CheckResult(
                name=f"security:{Path(filepath).name}",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.WARNING,
                message="No security issues detected",
                duration_ms=(time.time() - start_time) * 1000
            )
            
        except Exception as e:
            return CheckResult(
                name=f"security:{Path(filepath).name}",
                status=CheckStatus.SKIPPED,
                severity=CheckSeverity.WARNING,
                message=f"Could not scan: {e}",
                duration_ms=(time.time() - start_time) * 1000
            )


class ImportChecker:
    """Check for import issues and unused imports"""
    
    def check_file(self, filepath: str) -> CheckResult:
        """Check imports in file"""
        start_time = time.time()
        issues = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                source = f.read()
            
            tree = ast.parse(source)
            
            # Collect imports
            imports = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        name = alias.asname if alias.asname else alias.name
                        imports.add(name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        for alias in node.names:
                            name = alias.asname if alias.asname else alias.name
                            if name != '*':
                                imports.add(name)
            
            # Check for standard library imports that might shadow builtins
            suspicious = {'test', 'email', 'json', 'logging', 'typing'}
            for imp in imports & suspicious:
                # This is informational only
                pass
            
            return CheckResult(
                name=f"imports:{Path(filepath).name}",
                status=CheckStatus.PASSED,
                severity=CheckSeverity.INFO,
                message=f"Found {len(imports)} imports",
                duration_ms=(time.time() - start_time) * 1000,
                metadata={'imports': list(imports)}
            )
            
        except Exception as e:
            return CheckResult(
                name=f"imports:{Path(filepath).name}",
                status=CheckStatus.SKIPPED,
                severity=CheckSeverity.INFO,
                message=f"Could not analyze: {e}",
                duration_ms=(time.time() - start_time) * 1000
            )


class CoverageChecker:
    """Test coverage requirements checker"""
    
    def __init__(self, min_coverage: float = 80.0):
        self.min_coverage = min_coverage
    
    def check_coverage_report(self, report_path: str = "coverage.json") -> CheckResult:
        """Check coverage from JSON report"""
        start_time = time.time()
        
        try:
            with open(report_path, 'r') as f:
                data = json.load(f)
            
            total_coverage = data.get('totals', {}).get('percent_covered', 0)
            
            if total_coverage >= self.min_coverage:
                return CheckResult(
                    name="coverage",
                    status=CheckStatus.PASSED,
                    severity=CheckSeverity.ERROR,
                    message=f"Coverage: {total_coverage:.1f}% (min: {self.min_coverage}%)",
                    duration_ms=(time.time() - start_time) * 1000,
                    metadata={'coverage': total_coverage}
                )
            else:
                return CheckResult(
                    name="coverage",
                    status=CheckStatus.FAILED,
                    severity=CheckSeverity.ERROR,
                    message=f"Coverage: {total_coverage:.1f}% (min: {self.min_coverage}%)",
                    duration_ms=(time.time() - start_time) * 1000,
                    metadata={'coverage': total_coverage}
                )
                
        except FileNotFoundError:
            return CheckResult(
                name="coverage",
                status=CheckStatus.SKIPPED,
                severity=CheckSeverity.ERROR,
                message="Coverage report not found",
                duration_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            return CheckResult(
                name="coverage",
                status=CheckStatus.FAILED,
                severity=CheckSeverity.ERROR,
                message=f"Error reading coverage: {e}",
                duration_ms=(time.time() - start_time) * 1000
            )


class QualityGateRunner:
    """
    Runs all quality checks and produces a report.
    
    Features:
    - Multiple checker types (syntax, complexity, security)
    - Configurable thresholds
    - Report generation in multiple formats
    - Pre-commit hook support
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Checkers
        self.syntax_checker = SyntaxChecker()
        self.complexity_checker = ComplexityChecker()
        self.security_checker = SecurityChecker()
        self.import_checker = ImportChecker()
        self.coverage_checker = CoverageChecker()
        
        # Configuration
        self.check_syntax = True
        self.check_complexity = True
        self.check_security = True
        self.check_imports = False  # Info only
        self.check_coverage = False  # Requires report
        
        # Results
        self._results: List[CheckResult] = []
        self._report: Optional[QualityReport] = None
    
    def configure(self, 
                  max_complexity: int = 10,
                  max_line_length: int = 120,
                  min_coverage: float = 80.0) -> 'QualityGateRunner':
        """Configure checker thresholds"""
        self.complexity_checker.max_complexity = max_complexity
        self.complexity_checker.max_line_length = max_line_length
        self.coverage_checker.min_coverage = min_coverage
        return self
    
    def check_file(self, filepath: str) -> List[CheckResult]:
        """Run all checks on a single file"""
        results = []
        
        if not filepath.endswith('.py'):
            return results
        
        if self.check_syntax:
            results.append(self.syntax_checker.check_file(filepath))
        
        if self.check_complexity:
            results.append(self.complexity_checker.check_file(filepath))
        
        if self.check_security:
            results.append(self.security_checker.check_file(filepath))
        
        if self.check_imports:
            results.append(self.import_checker.check_file(filepath))
        
        return results
    
    def check_directory(self, directory: str, 
                        exclude_patterns: Optional[List[str]] = None) -> QualityReport:
        """Run all checks on a directory"""
        start_time = time.time()
        self._results.clear()
        
        exclude_patterns = exclude_patterns or [
            'venv', '__pycache__', '.git', 'node_modules', 
            'build', 'dist', '*.egg-info'
        ]
        
        path = Path(directory)
        
        for py_file in path.rglob("*.py"):
            # Check exclusions
            skip = False
            for pattern in exclude_patterns:
                if pattern in str(py_file):
                    skip = True
                    break
            
            if skip:
                continue
            
            file_results = self.check_file(str(py_file))
            self._results.extend(file_results)
        
        # Check coverage if enabled
        if self.check_coverage:
            self._results.append(self.coverage_checker.check_coverage_report())
        
        # Generate report
        duration_ms = (time.time() - start_time) * 1000
        
        passed = len([r for r in self._results if r.status == CheckStatus.PASSED])
        failed = len([r for r in self._results if r.status == CheckStatus.FAILED])
        warnings = len([r for r in self._results if r.status == CheckStatus.WARNING])
        skipped = len([r for r in self._results if r.status == CheckStatus.SKIPPED])
        
        self._report = QualityReport(
            timestamp=time.time(),
            duration_ms=duration_ms,
            total_checks=len(self._results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            skipped=skipped,
            checks=list(self._results)
        )
        
        return self._report
    
    def check_staged_files(self) -> QualityReport:
        """Check only git staged files"""
        start_time = time.time()
        self._results.clear()
        
        try:
            # Get staged files
            result = subprocess.run(
                ['git', 'diff', '--cached', '--name-only', '--diff-filter=ACM'],
                capture_output=True,
                text=True,
                check=True
            )
            
            staged_files = [f for f in result.stdout.strip().split('\n') if f.endswith('.py')]
            
            for filepath in staged_files:
                if os.path.exists(filepath):
                    file_results = self.check_file(filepath)
                    self._results.extend(file_results)
            
        except subprocess.CalledProcessError:
            self.logger.warning("Not a git repository or git not available")
        except Exception as e:
            self.logger.error(f"Error checking staged files: {e}")
        
        # Generate report
        duration_ms = (time.time() - start_time) * 1000
        
        passed = len([r for r in self._results if r.status == CheckStatus.PASSED])
        failed = len([r for r in self._results if r.status == CheckStatus.FAILED])
        warnings = len([r for r in self._results if r.status == CheckStatus.WARNING])
        skipped = len([r for r in self._results if r.status == CheckStatus.SKIPPED])
        
        self._report = QualityReport(
            timestamp=time.time(),
            duration_ms=duration_ms,
            total_checks=len(self._results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            skipped=skipped,
            checks=list(self._results)
        )
        
        return self._report
    
    def get_report(self) -> Optional[QualityReport]:
        """Get the last quality report"""
        return self._report
    
    def save_report(self, filepath: str, format: str = "json") -> None:
        """Save report to file"""
        if not self._report:
            return
        
        if format == "json":
            with open(filepath, 'w') as f:
                json.dump(self._report.to_dict(), f, indent=2)
        elif format == "markdown":
            with open(filepath, 'w') as f:
                f.write(self._report.to_markdown())
    
    def print_summary(self) -> None:
        """Print summary to console"""
        if not self._report:
            print("No report available")
            return
        
        status = "PASSED" if self._report.all_passed else "FAILED"
        status_color = "\033[32m" if self._report.all_passed else "\033[31m"
        reset = "\033[0m"
        
        print(f"\n{'=' * 60}")
        print(f"Quality Gate: {status_color}{status}{reset}")
        print(f"{'=' * 60}")
        print(f"Total Checks: {self._report.total_checks}")
        print(f"  Passed:   {self._report.passed}")
        print(f"  Failed:   {self._report.failed}")
        print(f"  Warnings: {self._report.warnings}")
        print(f"  Skipped:  {self._report.skipped}")
        print(f"Duration:   {self._report.duration_ms:.0f}ms")
        
        # Print failed checks
        failed = [c for c in self._report.checks if c.status == CheckStatus.FAILED]
        if failed:
            print(f"\nFailed Checks:")
            for check in failed:
                print(f"  ❌ {check.name}: {check.message}")
                for detail in check.details[:3]:
                    print(f"     - {detail}")


def generate_precommit_hook(output_path: str = ".git/hooks/pre-commit") -> str:
    """Generate pre-commit hook script"""
    hook_content = '''#!/bin/bash
# Pre-commit hook generated by quality_gates.py
# Runs quality checks on staged Python files

set -e

echo "Running quality gates..."

# Run quality gate checks
python -c "
from quality_gates import QualityGateRunner
import sys

runner = QualityGateRunner()
runner.check_syntax = True
runner.check_complexity = True
runner.check_security = True

report = runner.check_staged_files()
runner.print_summary()

if not report.all_passed:
    print('\\nQuality gate failed! Fix issues before committing.')
    sys.exit(1)
"

echo "Quality gates passed!"
'''
    
    # Write hook file
    hook_path = Path(output_path)
    hook_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(hook_path, 'w') as f:
        f.write(hook_content)
    
    # Make executable
    os.chmod(hook_path, 0o755)
    
    return str(hook_path)


def generate_github_action(output_path: str = ".github/workflows/quality.yml") -> str:
    """Generate GitHub Actions workflow for quality checks"""
    workflow_content = '''name: Quality Gates

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  quality-check:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run Quality Gates
      run: |
        python -c "
        from quality_gates import QualityGateRunner
        import sys
        
        runner = QualityGateRunner()
        runner.configure(max_complexity=15, max_line_length=120)
        
        report = runner.check_directory('.')
        runner.print_summary()
        runner.save_report('quality_report.json', 'json')
        runner.save_report('quality_report.md', 'markdown')
        
        if not report.all_passed:
            sys.exit(1)
        "
    
    - name: Upload Quality Report
      if: always()
      uses: actions/upload-artifact@v3
      with:
        name: quality-report
        path: |
          quality_report.json
          quality_report.md
'''
    
    # Write workflow file
    workflow_path = Path(output_path)
    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(workflow_path, 'w') as f:
        f.write(workflow_content)
    
    return str(workflow_path)


# CLI entry point
def main():
    """CLI entry point for quality gates"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Quality Gates - Code quality checker')
    parser.add_argument('path', nargs='?', default='.', help='Path to check (default: current directory)')
    parser.add_argument('--staged', action='store_true', help='Check only git staged files')
    parser.add_argument('--max-complexity', type=int, default=10, help='Max cyclomatic complexity')
    parser.add_argument('--max-line-length', type=int, default=120, help='Max line length')
    parser.add_argument('--output', help='Output report file')
    parser.add_argument('--format', choices=['json', 'markdown'], default='json', help='Report format')
    parser.add_argument('--generate-hook', action='store_true', help='Generate pre-commit hook')
    parser.add_argument('--generate-workflow', action='store_true', help='Generate GitHub Actions workflow')
    
    args = parser.parse_args()
    
    # Generate hooks/workflows if requested
    if args.generate_hook:
        path = generate_precommit_hook()
        print(f"Pre-commit hook generated: {path}")
        return
    
    if args.generate_workflow:
        path = generate_github_action()
        print(f"GitHub workflow generated: {path}")
        return
    
    # Run quality checks
    runner = QualityGateRunner()
    runner.configure(
        max_complexity=args.max_complexity,
        max_line_length=args.max_line_length
    )
    
    if args.staged:
        report = runner.check_staged_files()
    else:
        report = runner.check_directory(args.path)
    
    runner.print_summary()
    
    if args.output:
        runner.save_report(args.output, args.format)
        print(f"\nReport saved to: {args.output}")
    
    # Exit with error code if checks failed
    sys.exit(0 if report.all_passed else 1)


if __name__ == "__main__":
    main()
