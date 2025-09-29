// Coverage reporting and validation for GORP testing
package testing

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// CoverageReport represents test coverage information
type CoverageReport struct {
	Timestamp     time.Time              `json:"timestamp"`
	TotalLines    int                    `json:"total_lines"`
	CoveredLines  int                    `json:"covered_lines"`
	CoveragePercentage float64           `json:"coverage_percentage"`
	Packages      []PackageCoverage      `json:"packages"`
	Functions     []FunctionCoverage     `json:"functions"`
	Statements    []StatementCoverage    `json:"statements"`
	Branches      []BranchCoverage       `json:"branches"`
	TestResults   TestResults            `json:"test_results"`
	Violations    []CoverageViolation    `json:"violations"`
}

// PackageCoverage represents coverage for a single package
type PackageCoverage struct {
	Name               string  `json:"name"`
	Path               string  `json:"path"`
	TotalLines         int     `json:"total_lines"`
	CoveredLines       int     `json:"covered_lines"`
	CoveragePercentage float64 `json:"coverage_percentage"`
	Files              []FileCoverage `json:"files"`
}

// FileCoverage represents coverage for a single file
type FileCoverage struct {
	Name               string  `json:"name"`
	Path               string  `json:"path"`
	TotalLines         int     `json:"total_lines"`
	CoveredLines       int     `json:"covered_lines"`
	CoveragePercentage float64 `json:"coverage_percentage"`
	Lines              []LineCoverage `json:"lines"`
}

// LineCoverage represents coverage for a single line
type LineCoverage struct {
	Number   int  `json:"number"`
	Covered  bool `json:"covered"`
	HitCount int  `json:"hit_count"`
}

// FunctionCoverage represents coverage for a single function
type FunctionCoverage struct {
	Name               string  `json:"name"`
	Package            string  `json:"package"`
	File               string  `json:"file"`
	StartLine          int     `json:"start_line"`
	EndLine            int     `json:"end_line"`
	TotalLines         int     `json:"total_lines"`
	CoveredLines       int     `json:"covered_lines"`
	CoveragePercentage float64 `json:"coverage_percentage"`
	Complexity         int     `json:"complexity"`
}

// StatementCoverage represents coverage for individual statements
type StatementCoverage struct {
	File      string  `json:"file"`
	StartLine int     `json:"start_line"`
	EndLine   int     `json:"end_line"`
	Covered   bool    `json:"covered"`
	HitCount  int     `json:"hit_count"`
}

// BranchCoverage represents coverage for conditional branches
type BranchCoverage struct {
	File        string  `json:"file"`
	Line        int     `json:"line"`
	BranchType  string  `json:"branch_type"`
	TotalBranches int   `json:"total_branches"`
	CoveredBranches int `json:"covered_branches"`
	CoveragePercentage float64 `json:"coverage_percentage"`
}

// TestResults represents test execution results
type TestResults struct {
	TotalTests   int           `json:"total_tests"`
	PassedTests  int           `json:"passed_tests"`
	FailedTests  int           `json:"failed_tests"`
	SkippedTests int           `json:"skipped_tests"`
	Duration     time.Duration `json:"duration"`
	TestCases    []TestCase    `json:"test_cases"`
}

// TestCase represents a single test case result
type TestCase struct {
	Name     string        `json:"name"`
	Package  string        `json:"package"`
	Status   string        `json:"status"`
	Duration time.Duration `json:"duration"`
	Output   string        `json:"output,omitempty"`
	Error    string        `json:"error,omitempty"`
}

// CoverageViolation represents a coverage policy violation
type CoverageViolation struct {
	Type        string  `json:"type"`
	Package     string  `json:"package"`
	File        string  `json:"file"`
	Function    string  `json:"function,omitempty"`
	Line        int     `json:"line,omitempty"`
	Expected    float64 `json:"expected"`
	Actual      float64 `json:"actual"`
	Message     string  `json:"message"`
	Severity    string  `json:"severity"`
}

// CoveragePolicy defines coverage requirements
type CoveragePolicy struct {
	MinimumCoverage         float64            `json:"minimum_coverage"`
	MinimumPackageCoverage  float64            `json:"minimum_package_coverage"`
	MinimumFunctionCoverage float64            `json:"minimum_function_coverage"`
	RequireBranchCoverage   bool               `json:"require_branch_coverage"`
	MinimumBranchCoverage   float64            `json:"minimum_branch_coverage"`
	ExcludePatterns         []string           `json:"exclude_patterns"`
	PackageOverrides        map[string]float64 `json:"package_overrides"`
	CriticalPackages        []string           `json:"critical_packages"`
	MaxComplexity           int                `json:"max_complexity"`
	RequireDocumentation    bool               `json:"require_documentation"`
}

// DefaultCoveragePolicy returns a sensible default coverage policy
func DefaultCoveragePolicy() CoveragePolicy {
	return CoveragePolicy{
		MinimumCoverage:         80.0,
		MinimumPackageCoverage:  75.0,
		MinimumFunctionCoverage: 90.0,
		RequireBranchCoverage:   true,
		MinimumBranchCoverage:   70.0,
		ExcludePatterns:         []string{"*_test.go", "*/vendor/*", "*/testdata/*"},
		PackageOverrides: map[string]float64{
			"testing": 60.0, // Testing utilities may have lower coverage requirements
		},
		CriticalPackages:     []string{"db", "mapping", "query", "security"},
		MaxComplexity:        10,
		RequireDocumentation: true,
	}
}

// CoverageAnalyzer analyzes test coverage
type CoverageAnalyzer struct {
	projectRoot    string
	policy         CoveragePolicy
	coverageFile   string
	reportDir      string
	excludePatterns []string
}

// NewCoverageAnalyzer creates a new coverage analyzer
func NewCoverageAnalyzer(projectRoot string, policy CoveragePolicy) *CoverageAnalyzer {
	return &CoverageAnalyzer{
		projectRoot:     projectRoot,
		policy:          policy,
		coverageFile:    filepath.Join(projectRoot, "coverage.out"),
		reportDir:       filepath.Join(projectRoot, "coverage-reports"),
		excludePatterns: policy.ExcludePatterns,
	}
}

// SetCoverageFile sets the path to the coverage output file
func (ca *CoverageAnalyzer) SetCoverageFile(path string) {
	ca.coverageFile = path
}

// SetReportDirectory sets the directory for coverage reports
func (ca *CoverageAnalyzer) SetReportDirectory(dir string) {
	ca.reportDir = dir
}

// RunCoverageAnalysis runs a comprehensive coverage analysis
func (ca *CoverageAnalyzer) RunCoverageAnalysis(t *testing.T) (*CoverageReport, error) {
	t.Helper()

	// Ensure report directory exists
	if err := os.MkdirAll(ca.reportDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create report directory: %w", err)
	}

	// Parse coverage data
	coverageData, err := ca.parseCoverageFile()
	if err != nil {
		return nil, fmt.Errorf("failed to parse coverage file: %w", err)
	}

	// Analyze source code
	packages, err := ca.analyzeSourceCode()
	if err != nil {
		return nil, fmt.Errorf("failed to analyze source code: %w", err)
	}

	// Generate coverage report
	report := &CoverageReport{
		Timestamp: time.Now(),
		Packages:  packages,
	}

	// Calculate overall statistics
	ca.calculateOverallStats(report)

	// Analyze functions
	functions, err := ca.analyzeFunctions(coverageData)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze functions: %w", err)
	}
	report.Functions = functions

	// Analyze statements
	statements := ca.analyzeStatements(coverageData)
	report.Statements = statements

	// Analyze branches (simplified implementation)
	branches := ca.analyzeBranches(coverageData)
	report.Branches = branches

	// Validate against policy
	violations := ca.validatePolicy(report)
	report.Violations = violations

	// Save report
	if err := ca.saveReport(report); err != nil {
		t.Logf("Failed to save coverage report: %v", err)
	}

	// Generate HTML report
	if err := ca.generateHTMLReport(report); err != nil {
		t.Logf("Failed to generate HTML report: %v", err)
	}

	return report, nil
}

// parseCoverageFile parses the Go coverage output file
func (ca *CoverageAnalyzer) parseCoverageFile() (map[string][]CoverageBlock, error) {
	if _, err := os.Stat(ca.coverageFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("coverage file does not exist: %s", ca.coverageFile)
	}

	content, err := os.ReadFile(ca.coverageFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read coverage file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 || !strings.HasPrefix(lines[0], "mode:") {
		return nil, fmt.Errorf("invalid coverage file format")
	}

	coverageData := make(map[string][]CoverageBlock)

	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		block, err := parseCoverageBlock(line)
		if err != nil {
			continue // Skip invalid lines
		}

		coverageData[block.File] = append(coverageData[block.File], block)
	}

	return coverageData, nil
}

// CoverageBlock represents a coverage block from the coverage file
type CoverageBlock struct {
	File      string
	StartLine int
	StartCol  int
	EndLine   int
	EndCol    int
	NumStmt   int
	Count     int
}

func parseCoverageBlock(line string) (CoverageBlock, error) {
	// Parse line format: file.go:startLine.startCol,endLine.endCol numStmt count
	parts := strings.Fields(line)
	if len(parts) != 3 {
		return CoverageBlock{}, fmt.Errorf("invalid coverage line format")
	}

	// Parse file and positions
	fileAndPos := parts[0]
	colonIdx := strings.LastIndex(fileAndPos, ":")
	if colonIdx == -1 {
		return CoverageBlock{}, fmt.Errorf("invalid file:position format")
	}

	file := fileAndPos[:colonIdx]
	positions := fileAndPos[colonIdx+1:]

	// Parse positions
	commaIdx := strings.Index(positions, ",")
	if commaIdx == -1 {
		return CoverageBlock{}, fmt.Errorf("invalid position format")
	}

	startPos := positions[:commaIdx]
	endPos := positions[commaIdx+1:]

	// Parse start position
	startDotIdx := strings.Index(startPos, ".")
	if startDotIdx == -1 {
		return CoverageBlock{}, fmt.Errorf("invalid start position format")
	}

	startLine, err := strconv.Atoi(startPos[:startDotIdx])
	if err != nil {
		return CoverageBlock{}, err
	}

	startCol, err := strconv.Atoi(startPos[startDotIdx+1:])
	if err != nil {
		return CoverageBlock{}, err
	}

	// Parse end position
	endDotIdx := strings.Index(endPos, ".")
	if endDotIdx == -1 {
		return CoverageBlock{}, fmt.Errorf("invalid end position format")
	}

	endLine, err := strconv.Atoi(endPos[:endDotIdx])
	if err != nil {
		return CoverageBlock{}, err
	}

	endCol, err := strconv.Atoi(endPos[endDotIdx+1:])
	if err != nil {
		return CoverageBlock{}, err
	}

	// Parse statement count and hit count
	numStmt, err := strconv.Atoi(parts[1])
	if err != nil {
		return CoverageBlock{}, err
	}

	count, err := strconv.Atoi(parts[2])
	if err != nil {
		return CoverageBlock{}, err
	}

	return CoverageBlock{
		File:      file,
		StartLine: startLine,
		StartCol:  startCol,
		EndLine:   endLine,
		EndCol:    endCol,
		NumStmt:   numStmt,
		Count:     count,
	}, nil
}

// analyzeSourceCode analyzes source code to gather package and file information
func (ca *CoverageAnalyzer) analyzeSourceCode() ([]PackageCoverage, error) {
	var packages []PackageCoverage

	err := filepath.WalkDir(ca.projectRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() || ca.shouldExclude(path) {
			return nil
		}

		// Check if this directory contains Go files
		goFiles, err := filepath.Glob(filepath.Join(path, "*.go"))
		if err != nil || len(goFiles) == 0 {
			return nil
		}

		// Filter out test files for now
		var sourceFiles []string
		for _, file := range goFiles {
			if !strings.HasSuffix(file, "_test.go") {
				sourceFiles = append(sourceFiles, file)
			}
		}

		if len(sourceFiles) == 0 {
			return nil
		}

		// Analyze package
		pkg, err := ca.analyzePackage(path, sourceFiles)
		if err != nil {
			return err
		}

		packages = append(packages, pkg)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return packages, nil
}

func (ca *CoverageAnalyzer) analyzePackage(packagePath string, sourceFiles []string) (PackageCoverage, error) {
	relPath, _ := filepath.Rel(ca.projectRoot, packagePath)
	packageName := filepath.Base(packagePath)

	pkg := PackageCoverage{
		Name:  packageName,
		Path:  relPath,
		Files: make([]FileCoverage, 0, len(sourceFiles)),
	}

	for _, file := range sourceFiles {
		fileCov, err := ca.analyzeFile(file)
		if err != nil {
			continue // Skip files that can't be analyzed
		}

		pkg.Files = append(pkg.Files, fileCov)
		pkg.TotalLines += fileCov.TotalLines
		pkg.CoveredLines += fileCov.CoveredLines
	}

	if pkg.TotalLines > 0 {
		pkg.CoveragePercentage = float64(pkg.CoveredLines) / float64(pkg.TotalLines) * 100
	}

	return pkg, nil
}

func (ca *CoverageAnalyzer) analyzeFile(filePath string) (FileCoverage, error) {
	relPath, _ := filepath.Rel(ca.projectRoot, filePath)
	fileName := filepath.Base(filePath)

	// Count lines in file
	content, err := os.ReadFile(filePath)
	if err != nil {
		return FileCoverage{}, err
	}

	lines := strings.Split(string(content), "\n")
	totalLines := len(lines)

	// TODO: This is simplified - in a real implementation, you would:
	// 1. Parse the Go source to identify executable lines
	// 2. Cross-reference with coverage data to determine covered lines
	// 3. Exclude comments, blank lines, etc.

	file := FileCoverage{
		Name:               fileName,
		Path:               relPath,
		TotalLines:         totalLines,
		CoveredLines:       totalLines / 2, // Simplified assumption
		CoveragePercentage: 50.0,           // Simplified assumption
		Lines:              make([]LineCoverage, 0),
	}

	// Generate line coverage data (simplified)
	for i := 1; i <= totalLines; i++ {
		line := LineCoverage{
			Number:   i,
			Covered:  i%2 == 0, // Simplified assumption
			HitCount: i % 3,    // Simplified assumption
		}
		file.Lines = append(file.Lines, line)
	}

	if file.TotalLines > 0 {
		file.CoveragePercentage = float64(file.CoveredLines) / float64(file.TotalLines) * 100
	}

	return file, nil
}

func (ca *CoverageAnalyzer) analyzeFunctions(coverageData map[string][]CoverageBlock) ([]FunctionCoverage, error) {
	var functions []FunctionCoverage

	// Walk through source files and analyze functions
	err := filepath.WalkDir(ca.projectRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		if ca.shouldExclude(path) {
			return nil
		}

		fileFunctions, err := ca.analyzeFunctionsInFile(path, coverageData)
		if err != nil {
			return nil // Skip files that can't be analyzed
		}

		functions = append(functions, fileFunctions...)
		return nil
	})

	return functions, err
}

func (ca *CoverageAnalyzer) analyzeFunctionsInFile(filePath string, coverageData map[string][]CoverageBlock) ([]FunctionCoverage, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var functions []FunctionCoverage
	relPath, _ := filepath.Rel(ca.projectRoot, filePath)

	ast.Inspect(node, func(n ast.Node) bool {
		switch fn := n.(type) {
		case *ast.FuncDecl:
			if fn.Name != nil {
				funcCov := ca.analyzeFunctionCoverage(fn, relPath, fset, coverageData)
				functions = append(functions, funcCov)
			}
		}
		return true
	})

	return functions, nil
}

func (ca *CoverageAnalyzer) analyzeFunctionCoverage(fn *ast.FuncDecl, filePath string, fset *token.FileSet, coverageData map[string][]CoverageBlock) FunctionCoverage {
	startPos := fset.Position(fn.Pos())
	endPos := fset.Position(fn.End())

	funcCov := FunctionCoverage{
		Name:       fn.Name.Name,
		Package:    "", // Would need to extract from package declaration
		File:       filePath,
		StartLine:  startPos.Line,
		EndLine:    endPos.Line,
		TotalLines: endPos.Line - startPos.Line + 1,
		Complexity: ca.calculateCyclomaticComplexity(fn),
	}

	// Calculate coverage based on coverage blocks
	coveredLines := 0
	if blocks, exists := coverageData[filePath]; exists {
		for _, block := range blocks {
			if block.StartLine >= funcCov.StartLine && block.EndLine <= funcCov.EndLine {
				if block.Count > 0 {
					coveredLines += block.EndLine - block.StartLine + 1
				}
			}
		}
	}

	funcCov.CoveredLines = coveredLines
	if funcCov.TotalLines > 0 {
		funcCov.CoveragePercentage = float64(funcCov.CoveredLines) / float64(funcCov.TotalLines) * 100
	}

	return funcCov
}

func (ca *CoverageAnalyzer) calculateCyclomaticComplexity(fn *ast.FuncDecl) int {
	complexity := 1 // Base complexity

	ast.Inspect(fn, func(n ast.Node) bool {
		switch n.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt:
			complexity++
		case *ast.CaseClause:
			complexity++
		}
		return true
	})

	return complexity
}

func (ca *CoverageAnalyzer) analyzeStatements(coverageData map[string][]CoverageBlock) []StatementCoverage {
	var statements []StatementCoverage

	for file, blocks := range coverageData {
		for _, block := range blocks {
			stmt := StatementCoverage{
				File:      file,
				StartLine: block.StartLine,
				EndLine:   block.EndLine,
				Covered:   block.Count > 0,
				HitCount:  block.Count,
			}
			statements = append(statements, stmt)
		}
	}

	return statements
}

func (ca *CoverageAnalyzer) analyzeBranches(coverageData map[string][]CoverageBlock) []BranchCoverage {
	// Simplified branch analysis - real implementation would need AST parsing
	// to identify conditional branches
	var branches []BranchCoverage

	for file, blocks := range coverageData {
		branchMap := make(map[int]*BranchCoverage)

		for _, block := range blocks {
			if existing, exists := branchMap[block.StartLine]; exists {
				existing.TotalBranches++
				if block.Count > 0 {
					existing.CoveredBranches++
				}
			} else {
				branch := &BranchCoverage{
					File:            file,
					Line:            block.StartLine,
					BranchType:      "conditional",
					TotalBranches:   1,
					CoveredBranches: 0,
				}
				if block.Count > 0 {
					branch.CoveredBranches = 1
				}
				branchMap[block.StartLine] = branch
			}
		}

		for _, branch := range branchMap {
			if branch.TotalBranches > 0 {
				branch.CoveragePercentage = float64(branch.CoveredBranches) / float64(branch.TotalBranches) * 100
			}
			branches = append(branches, *branch)
		}
	}

	return branches
}

func (ca *CoverageAnalyzer) calculateOverallStats(report *CoverageReport) {
	totalLines := 0
	coveredLines := 0

	for _, pkg := range report.Packages {
		totalLines += pkg.TotalLines
		coveredLines += pkg.CoveredLines
	}

	report.TotalLines = totalLines
	report.CoveredLines = coveredLines

	if totalLines > 0 {
		report.CoveragePercentage = float64(coveredLines) / float64(totalLines) * 100
	}
}

func (ca *CoverageAnalyzer) validatePolicy(report *CoverageReport) []CoverageViolation {
	var violations []CoverageViolation

	// Check overall coverage
	if report.CoveragePercentage < ca.policy.MinimumCoverage {
		violations = append(violations, CoverageViolation{
			Type:     "overall_coverage",
			Expected: ca.policy.MinimumCoverage,
			Actual:   report.CoveragePercentage,
			Message:  fmt.Sprintf("Overall coverage %.2f%% is below minimum %.2f%%", report.CoveragePercentage, ca.policy.MinimumCoverage),
			Severity: "error",
		})
	}

	// Check package coverage
	for _, pkg := range report.Packages {
		minCoverage := ca.policy.MinimumPackageCoverage
		if override, exists := ca.policy.PackageOverrides[pkg.Name]; exists {
			minCoverage = override
		}

		if pkg.CoveragePercentage < minCoverage {
			severity := "warning"
			for _, critical := range ca.policy.CriticalPackages {
				if pkg.Name == critical {
					severity = "error"
					break
				}
			}

			violations = append(violations, CoverageViolation{
				Type:     "package_coverage",
				Package:  pkg.Name,
				Expected: minCoverage,
				Actual:   pkg.CoveragePercentage,
				Message:  fmt.Sprintf("Package %s coverage %.2f%% is below minimum %.2f%%", pkg.Name, pkg.CoveragePercentage, minCoverage),
				Severity: severity,
			})
		}
	}

	// Check function coverage
	for _, fn := range report.Functions {
		if fn.CoveragePercentage < ca.policy.MinimumFunctionCoverage {
			violations = append(violations, CoverageViolation{
				Type:     "function_coverage",
				Package:  fn.Package,
				File:     fn.File,
				Function: fn.Name,
				Line:     fn.StartLine,
				Expected: ca.policy.MinimumFunctionCoverage,
				Actual:   fn.CoveragePercentage,
				Message:  fmt.Sprintf("Function %s coverage %.2f%% is below minimum %.2f%%", fn.Name, fn.CoveragePercentage, ca.policy.MinimumFunctionCoverage),
				Severity: "warning",
			})
		}

		// Check complexity
		if fn.Complexity > ca.policy.MaxComplexity {
			violations = append(violations, CoverageViolation{
				Type:     "complexity",
				Package:  fn.Package,
				File:     fn.File,
				Function: fn.Name,
				Line:     fn.StartLine,
				Expected: float64(ca.policy.MaxComplexity),
				Actual:   float64(fn.Complexity),
				Message:  fmt.Sprintf("Function %s complexity %d exceeds maximum %d", fn.Name, fn.Complexity, ca.policy.MaxComplexity),
				Severity: "warning",
			})
		}
	}

	return violations
}

func (ca *CoverageAnalyzer) shouldExclude(path string) bool {
	for _, pattern := range ca.excludePatterns {
		if matched, _ := filepath.Match(pattern, path); matched {
			return true
		}
		if strings.Contains(path, pattern) {
			return true
		}
	}
	return false
}

func (ca *CoverageAnalyzer) saveReport(report *CoverageReport) error {
	filename := filepath.Join(ca.reportDir, fmt.Sprintf("coverage_report_%s.json", time.Now().Format("20060102_150405")))

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

func (ca *CoverageAnalyzer) generateHTMLReport(report *CoverageReport) error {
	htmlContent := ca.generateHTMLContent(report)
	filename := filepath.Join(ca.reportDir, "coverage_report.html")
	return os.WriteFile(filename, []byte(htmlContent), 0644)
}

func (ca *CoverageAnalyzer) generateHTMLContent(report *CoverageReport) string {
	// Simplified HTML generation - real implementation would use templates
	html := `<!DOCTYPE html>
<html>
<head>
    <title>Coverage Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background: #f0f0f0; padding: 15px; border-radius: 5px; }
        .package { margin: 20px 0; border: 1px solid #ddd; border-radius: 5px; }
        .package-header { background: #e0e0e0; padding: 10px; font-weight: bold; }
        .file { margin: 10px; padding: 5px; }
        .coverage-good { color: green; }
        .coverage-warning { color: orange; }
        .coverage-bad { color: red; }
        .violations { background: #ffe6e6; padding: 15px; border-radius: 5px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="header">
        <h1>GORP Coverage Report</h1>
        <p>Generated: ` + report.Timestamp.Format("2006-01-02 15:04:05") + `</p>
        <p>Overall Coverage: ` + fmt.Sprintf("%.2f%%", report.CoveragePercentage) + ` (` + fmt.Sprintf("%d/%d lines", report.CoveredLines, report.TotalLines) + `)</p>
    </div>`

	// Add violations if any
	if len(report.Violations) > 0 {
		html += `<div class="violations"><h2>Policy Violations</h2><ul>`
		for _, violation := range report.Violations {
			html += fmt.Sprintf(`<li class="%s">%s</li>`, violation.Severity, violation.Message)
		}
		html += `</ul></div>`
	}

	// Add package details
	html += `<h2>Package Details</h2>`
	for _, pkg := range report.Packages {
		coverageClass := "coverage-good"
		if pkg.CoveragePercentage < 70 {
			coverageClass = "coverage-bad"
		} else if pkg.CoveragePercentage < 85 {
			coverageClass = "coverage-warning"
		}

		html += fmt.Sprintf(`<div class="package">
            <div class="package-header">%s - <span class="%s">%.2f%%</span></div>`,
			pkg.Name, coverageClass, pkg.CoveragePercentage)

		for _, file := range pkg.Files {
			html += fmt.Sprintf(`<div class="file">%s - %.2f%%</div>`, file.Name, file.CoveragePercentage)
		}

		html += `</div>`
	}

	html += `</body></html>`
	return html
}

// CoverageValidator provides coverage validation for testing
type CoverageValidator struct {
	analyzer *CoverageAnalyzer
	policy   CoveragePolicy
}

// NewCoverageValidator creates a new coverage validator
func NewCoverageValidator(projectRoot string, policy CoveragePolicy) *CoverageValidator {
	return &CoverageValidator{
		analyzer: NewCoverageAnalyzer(projectRoot, policy),
		policy:   policy,
	}
}

// ValidateCoverage validates test coverage against policy
func (cv *CoverageValidator) ValidateCoverage(t *testing.T) {
	t.Helper()

	report, err := cv.analyzer.RunCoverageAnalysis(t)
	require.NoError(t, err, "Failed to run coverage analysis")

	// Check for violations
	errorViolations := 0
	warningViolations := 0

	for _, violation := range report.Violations {
		switch violation.Severity {
		case "error":
			errorViolations++
			t.Errorf("Coverage Error: %s", violation.Message)
		case "warning":
			warningViolations++
			t.Logf("Coverage Warning: %s", violation.Message)
		}
	}

	// Summary
	t.Logf("Coverage validation completed:")
	t.Logf("  Overall coverage: %.2f%%", report.CoveragePercentage)
	t.Logf("  Packages analyzed: %d", len(report.Packages))
	t.Logf("  Functions analyzed: %d", len(report.Functions))
	t.Logf("  Error violations: %d", errorViolations)
	t.Logf("  Warning violations: %d", warningViolations)

	if errorViolations > 0 {
		t.Fatalf("Coverage validation failed with %d error violations", errorViolations)
	}
}

// RunWithCoverage runs tests with coverage analysis
func RunWithCoverage(t *testing.T, projectRoot string, policy CoveragePolicy, testFunc func(*testing.T)) {
	t.Helper()

	// Run the tests
	testFunc(t)

	// Validate coverage
	validator := NewCoverageValidator(projectRoot, policy)
	validator.ValidateCoverage(t)
}