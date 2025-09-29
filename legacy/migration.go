package legacy

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"regexp"
	"strings"
	"time"
)

// MigrationTransformer provides utilities to transform old GORP code to new API
type MigrationTransformer struct {
	fileSet       *token.FileSet
	transformations []Transformation
	config        *TransformationConfig
}

// Transformation represents a code transformation rule
type Transformation struct {
	Name        string
	Pattern     *regexp.Regexp
	Replacement string
	Description string
	Example     TransformationExample
}

// TransformationExample shows before and after code
type TransformationExample struct {
	Before string
	After  string
}

// TransformationConfig controls transformation behavior
type TransformationConfig struct {
	AddImports          bool
	PreserveComments    bool
	AddMigrationComments bool
	TargetGoVersion     string
	EnableGenerics      bool
	EnableSQLX         bool
	EnableContextAware  bool
}

// DefaultTransformationConfig returns sensible defaults
func DefaultTransformationConfig() *TransformationConfig {
	return &TransformationConfig{
		AddImports:          true,
		PreserveComments:    true,
		AddMigrationComments: true,
		TargetGoVersion:     "1.24",
		EnableGenerics:      true,
		EnableSQLX:         true,
		EnableContextAware:  true,
	}
}

// NewMigrationTransformer creates a new migration transformer
func NewMigrationTransformer(config *TransformationConfig) *MigrationTransformer {
	if config == nil {
		config = DefaultTransformationConfig()
	}

	transformer := &MigrationTransformer{
		fileSet: token.NewFileSet(),
		config:  config,
	}

	transformer.setupTransformations()
	return transformer
}

// setupTransformations defines all the transformation rules
func (mt *MigrationTransformer) setupTransformations() {
	mt.transformations = []Transformation{
		{
			Name:        "DbMap Creation",
			Pattern:     regexp.MustCompile(`&gorp\.DbMap\{[^}]*\}`),
			Replacement: "legacy.NewLegacyDbMap(db, dialect, nil)",
			Description: "Transform DbMap creation to use compatibility wrapper",
			Example: TransformationExample{
				Before: `dbmap := &gorp.DbMap{Db: db, Dialect: dialect}`,
				After:  `dbmap := legacy.NewLegacyDbMap(db, dialect, nil)`,
			},
		},
		{
			Name:        "AddTable to Generic RegisterTable",
			Pattern:     regexp.MustCompile(`\.AddTable\(([^)]+)\)`),
			Replacement: ".RegisterTable[$1]()",
			Description: "Transform AddTable to generic RegisterTable for type safety",
			Example: TransformationExample{
				Before: `dbmap.AddTable(User{})`,
				After:  `mapper.RegisterTable[User]()`,
			},
		},
		{
			Name:        "AddTableWithName to Generic WithName",
			Pattern:     regexp.MustCompile(`\.AddTableWithName\(([^,]+),\s*"([^"]+)"\)`),
			Replacement: ".RegisterTable[$1]().WithName(\"$2\")",
			Description: "Transform AddTableWithName to generic RegisterTable with WithName",
			Example: TransformationExample{
				Before: `dbmap.AddTableWithName(User{}, "users")`,
				After:  `mapper.RegisterTable[User]().WithName("users")`,
			},
		},
		{
			Name:        "Get to Generic Get",
			Pattern:     regexp.MustCompile(`\.Get\(([^,]+),\s*([^)]+)\)`),
			Replacement: ".Get[${1}Type](ctx, $2)",
			Description: "Transform Get to generic type-safe Get",
			Example: TransformationExample{
				Before: `user, err := dbmap.Get(&User{}, id)`,
				After:  `user, err := builder.Get[User](ctx, id)`,
			},
		},
		{
			Name:        "Insert to Generic Insert",
			Pattern:     regexp.MustCompile(`\.Insert\(([^)]+)\)`),
			Replacement: ".Insert[${1}Type](ctx, $1)",
			Description: "Transform Insert to generic type-safe Insert",
			Example: TransformationExample{
				Before: `err := dbmap.Insert(user)`,
				After:  `err := builder.Insert[User](ctx, user)`,
			},
		},
		{
			Name:        "Update to Generic Update",
			Pattern:     regexp.MustCompile(`\.Update\(([^)]+)\)`),
			Replacement: ".Update[${1}Type](ctx, $1)",
			Description: "Transform Update to generic type-safe Update",
			Example: TransformationExample{
				Before: `count, err := dbmap.Update(user)`,
				After:  `count, err := builder.Update[User](ctx, user)`,
			},
		},
		{
			Name:        "Delete to Generic Delete",
			Pattern:     regexp.MustCompile(`\.Delete\(([^)]+)\)`),
			Replacement: ".Delete[${1}Type](ctx, $1)",
			Description: "Transform Delete to generic type-safe Delete",
			Example: TransformationExample{
				Before: `count, err := dbmap.Delete(user)`,
				After:  `count, err := builder.Delete[User](ctx, user)`,
			},
		},
		{
			Name:        "Select to Generic Query",
			Pattern:     regexp.MustCompile(`\.Select\(([^,]+),\s*"([^"]+)"([^)]*)\)`),
			Replacement: ".Query[${1}Type](ctx, \"$2\"$3)",
			Description: "Transform Select to generic type-safe Query",
			Example: TransformationExample{
				Before: `users, err := dbmap.Select(&User{}, "SELECT * FROM users WHERE active = ?", true)`,
				After:  `users, err := builder.Query[User](ctx, "SELECT * FROM users WHERE active = ?", true)`,
			},
		},
		{
			Name:        "SelectOne to Generic QueryOne",
			Pattern:     regexp.MustCompile(`\.SelectOne\(([^,]+),\s*"([^"]+)"([^)]*)\)`),
			Replacement: ".QueryOne[${1}Type](ctx, \"$2\"$3)",
			Description: "Transform SelectOne to generic type-safe QueryOne",
			Example: TransformationExample{
				Before: `err := dbmap.SelectOne(&user, "SELECT * FROM users WHERE id = ?", id)`,
				After:  `user, err := builder.QueryOne[User](ctx, "SELECT * FROM users WHERE id = ?", id)`,
			},
		},
		{
			Name:        "SelectInt to Generic QuerySingle",
			Pattern:     regexp.MustCompile(`\.SelectInt\("([^"]+)"([^)]*)\)`),
			Replacement: ".QuerySingle[int64](ctx, \"$1\"$2)",
			Description: "Transform SelectInt to generic QuerySingle",
			Example: TransformationExample{
				Before: `count, err := dbmap.SelectInt("SELECT COUNT(*) FROM users")`,
				After:  `count, err := builder.QuerySingle[int64](ctx, "SELECT COUNT(*) FROM users")`,
			},
		},
		{
			Name:        "SelectStr to Generic QuerySingle",
			Pattern:     regexp.MustCompile(`\.SelectStr\("([^"]+)"([^)]*)\)`),
			Replacement: ".QuerySingle[string](ctx, \"$1\"$2)",
			Description: "Transform SelectStr to generic QuerySingle",
			Example: TransformationExample{
				Before: `name, err := dbmap.SelectStr("SELECT name FROM users WHERE id = ?", id)`,
				After:  `name, err := builder.QuerySingle[string](ctx, "SELECT name FROM users WHERE id = ?", id)`,
			},
		},
		{
			Name:        "Transaction Begin to Context-aware",
			Pattern:     regexp.MustCompile(`\.Begin\(\)`),
			Replacement: ".BeginTx(ctx, nil)",
			Description: "Transform Begin to context-aware BeginTx",
			Example: TransformationExample{
				Before: `tx, err := dbmap.Begin()`,
				After:  `tx, err := conn.BeginTx(ctx, nil)`,
			},
		},
		{
			Name:        "Raw Exec to Context-aware",
			Pattern:     regexp.MustCompile(`\.Exec\("([^"]+)"([^)]*)\)`),
			Replacement: ".Exec(ctx, \"$1\"$2)",
			Description: "Transform Exec to context-aware execution",
			Example: TransformationExample{
				Before: `result, err := dbmap.Exec("DELETE FROM users WHERE inactive = ?", true)`,
				After:  `result, err := conn.Exec(ctx, "DELETE FROM users WHERE inactive = ?", true)`,
			},
		},
	}
}

// TransformFile transforms a Go source file from old GORP API to new API
func (mt *MigrationTransformer) TransformFile(filePath string) (*TransformResult, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	return mt.TransformCode(string(content), filePath)
}

// TransformCode transforms Go source code from old GORP API to new API
func (mt *MigrationTransformer) TransformCode(code, filename string) (*TransformResult, error) {
	result := &TransformResult{
		OriginalCode:     code,
		TransformedCode:  code,
		Filename:        filename,
		Transformations: make([]AppliedTransformation, 0),
		Timestamp:       time.Now(),
	}

	// Parse the source code
	file, err := parser.ParseFile(mt.fileSet, filename, code, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse file %s: %w", filename, err)
	}

	// Apply text-based transformations first
	transformedCode := code
	for _, transform := range mt.transformations {
		matches := transform.Pattern.FindAllStringSubmatch(transformedCode, -1)
		if len(matches) > 0 {
			newCode := transform.Pattern.ReplaceAllString(transformedCode, transform.Replacement)
			if newCode != transformedCode {
				result.Transformations = append(result.Transformations, AppliedTransformation{
					Name:        transform.Name,
					Description: transform.Description,
					Before:      transform.Example.Before,
					After:       transform.Example.After,
					LineNumber:  mt.findLineNumber(transformedCode, matches[0][0]),
				})
				transformedCode = newCode
			}
		}
	}

	result.TransformedCode = transformedCode

	// Apply AST-based transformations
	err = mt.applyASTTransformations(file, result)
	if err != nil {
		return result, fmt.Errorf("failed to apply AST transformations: %w", err)
	}

	// Add necessary imports
	if mt.config.AddImports {
		result.TransformedCode = mt.addRequiredImports(result.TransformedCode)
	}

	// Add migration comments
	if mt.config.AddMigrationComments {
		result.TransformedCode = mt.addMigrationComments(result.TransformedCode)
	}

	return result, nil
}

// TransformResult contains the results of a code transformation
type TransformResult struct {
	OriginalCode     string
	TransformedCode  string
	Filename        string
	Transformations []AppliedTransformation
	Warnings        []TransformWarning
	Timestamp       time.Time
}

// AppliedTransformation represents a transformation that was applied
type AppliedTransformation struct {
	Name        string
	Description string
	Before      string
	After       string
	LineNumber  int
}

// TransformWarning represents a potential issue found during transformation
type TransformWarning struct {
	Message    string
	LineNumber int
	Suggestion string
}

// applyASTTransformations applies more complex transformations using AST manipulation
func (mt *MigrationTransformer) applyASTTransformations(file *ast.File, result *TransformResult) error {
	// AST visitor to find and transform specific patterns
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.GenDecl:
			// Handle import declarations
			mt.handleImports(node, result)
		case *ast.CallExpr:
			// Handle method calls that need transformation
			mt.handleMethodCalls(node, result)
		case *ast.CompositeLit:
			// Handle struct literals (like &gorp.DbMap{})
			mt.handleCompositeLiterals(node, result)
		}
		return true
	})

	return nil
}

// handleImports processes import declarations and suggests new imports
func (mt *MigrationTransformer) handleImports(node *ast.GenDecl, result *TransformResult) {
	if node.Tok != token.IMPORT {
		return
	}

	for _, spec := range node.Specs {
		if importSpec, ok := spec.(*ast.ImportSpec); ok {
			if importSpec.Path != nil {
				importPath := strings.Trim(importSpec.Path.Value, `"`)

				// Detect old GORP import
				if strings.Contains(importPath, "gorp") && !strings.Contains(importPath, "legacy") {
					result.Warnings = append(result.Warnings, TransformWarning{
						Message:    fmt.Sprintf("Consider updating import %s to use new GORP packages", importPath),
						LineNumber: mt.fileSet.Position(importSpec.Pos()).Line,
						Suggestion: "Add imports for legacy, mapping, query, and db packages",
					})
				}
			}
		}
	}
}

// handleMethodCalls processes method calls for transformation
func (mt *MigrationTransformer) handleMethodCalls(node *ast.CallExpr, result *TransformResult) {
	if selectorExpr, ok := node.Fun.(*ast.SelectorExpr); ok {
		methodName := selectorExpr.Sel.Name

		// Check for deprecated methods
		deprecatedMethods := map[string]string{
			"AddTable":           "RegisterTable[T]",
			"AddTableWithName":   "RegisterTable[T]().WithName()",
			"Get":                "Get[T]",
			"Insert":             "Insert[T]",
			"Update":             "Update[T]",
			"Delete":             "Delete[T]",
			"Select":             "Query[T]",
			"SelectOne":          "QueryOne[T]",
			"SelectInt":          "QuerySingle[int64]",
			"SelectStr":          "QuerySingle[string]",
			"SelectFloat":        "QuerySingle[float64]",
			"Begin":              "BeginTx",
			"Exec":               "Exec with context",
		}

		if newMethod, isDeprecated := deprecatedMethods[methodName]; isDeprecated {
			result.Warnings = append(result.Warnings, TransformWarning{
				Message:    fmt.Sprintf("Method %s is deprecated", methodName),
				LineNumber: mt.fileSet.Position(node.Pos()).Line,
				Suggestion: fmt.Sprintf("Use %s instead for better type safety", newMethod),
			})
		}
	}
}

// handleCompositeLiterals processes struct literals for transformation
func (mt *MigrationTransformer) handleCompositeLiterals(node *ast.CompositeLit, result *TransformResult) {
	if selectorExpr, ok := node.Type.(*ast.SelectorExpr); ok {
		if ident, ok := selectorExpr.X.(*ast.Ident); ok {
			// Check for &gorp.DbMap{} patterns
			if ident.Name == "gorp" && selectorExpr.Sel.Name == "DbMap" {
				result.Warnings = append(result.Warnings, TransformWarning{
					Message:    "Direct DbMap creation detected",
					LineNumber: mt.fileSet.Position(node.Pos()).Line,
					Suggestion: "Use legacy.NewLegacyDbMap() for backward compatibility or new connection manager for modern API",
				})
			}
		}
	}
}

// findLineNumber finds the line number of a match in the source code
func (mt *MigrationTransformer) findLineNumber(code, match string) int {
	lines := strings.Split(code, "\n")
	for i, line := range lines {
		if strings.Contains(line, match) {
			return i + 1
		}
	}
	return 0
}

// addRequiredImports adds necessary imports for the new API
func (mt *MigrationTransformer) addRequiredImports(code string) string {
	// Check if imports are already present
	hasLegacy := strings.Contains(code, `"github.com/fathiraz/gorp/legacy"`)
	hasMapping := strings.Contains(code, `"github.com/fathiraz/gorp/mapping"`)
	hasQuery := strings.Contains(code, `"github.com/fathiraz/gorp/query"`)
	hasDB := strings.Contains(code, `"github.com/fathiraz/gorp/db"`)
	hasContext := strings.Contains(code, `"context"`)

	var newImports []string

	if !hasContext && (mt.config.EnableContextAware || mt.config.EnableSQLX) {
		newImports = append(newImports, `"context"`)
	}

	if !hasLegacy {
		newImports = append(newImports, `"github.com/fathiraz/gorp/legacy"`)
	}

	if !hasMapping && mt.config.EnableGenerics {
		newImports = append(newImports, `"github.com/fathiraz/gorp/mapping"`)
	}

	if !hasQuery && mt.config.EnableGenerics {
		newImports = append(newImports, `"github.com/fathiraz/gorp/query"`)
	}

	if !hasDB && mt.config.EnableSQLX {
		newImports = append(newImports, `"github.com/fathiraz/gorp/db"`)
	}

	if len(newImports) > 0 {
		// Add imports after existing import block
		importPattern := regexp.MustCompile(`import\s*\([^)]*\)`)
		if importPattern.MatchString(code) {
			// Add to existing import block
			replacement := fmt.Sprintf("import (\n\t%s", strings.Join(newImports, "\n\t"))
			code = importPattern.ReplaceAllStringFunc(code, func(match string) string {
				return strings.Replace(match, "import (", replacement, 1)
			})
		} else {
			// Add new import block after package declaration
			packagePattern := regexp.MustCompile(`(package\s+\w+\n)`)
			if packagePattern.MatchString(code) {
				importBlock := fmt.Sprintf("\nimport (\n\t%s\n)\n", strings.Join(newImports, "\n\t"))
				code = packagePattern.ReplaceAllString(code, "$1"+importBlock)
			}
		}
	}

	return code
}

// addMigrationComments adds helpful migration comments
func (mt *MigrationTransformer) addMigrationComments(code string) string {
	header := fmt.Sprintf(`// Code transformed by GORP Migration Tool on %s
// This file has been automatically updated to use the new GORP API
// Review the changes and test thoroughly before deploying to production
//
// Migration features enabled:
// - Generics: %t
// - SQLX: %t
// - Context-aware: %t
//
// For more information, see: https://github.com/go-gorp/gorp/blob/v3/docs/migration.md

`,
		time.Now().Format("2006-01-02 15:04:05"),
		mt.config.EnableGenerics,
		mt.config.EnableSQLX,
		mt.config.EnableContextAware,
	)

	// Add header after package declaration
	packagePattern := regexp.MustCompile(`(package\s+\w+\n)`)
	if packagePattern.MatchString(code) {
		code = packagePattern.ReplaceAllString(code, "$1\n"+header)
	}

	return code
}

// GenerateMigrationReport generates a detailed migration report
func (mt *MigrationTransformer) GenerateMigrationReport(results []*TransformResult) string {
	var report strings.Builder

	report.WriteString("# GORP Migration Report\n\n")
	report.WriteString(fmt.Sprintf("Generated on: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	report.WriteString(fmt.Sprintf("Files processed: %d\n\n", len(results)))

	// Summary statistics
	totalTransformations := 0
	totalWarnings := 0
	for _, result := range results {
		totalTransformations += len(result.Transformations)
		totalWarnings += len(result.Warnings)
	}

	report.WriteString("## Summary\n\n")
	report.WriteString(fmt.Sprintf("- Total transformations applied: %d\n", totalTransformations))
	report.WriteString(fmt.Sprintf("- Total warnings generated: %d\n\n", totalWarnings))

	// Detailed results for each file
	for _, result := range results {
		report.WriteString(fmt.Sprintf("## File: %s\n\n", result.Filename))

		if len(result.Transformations) > 0 {
			report.WriteString("### Transformations Applied\n\n")
			for _, transform := range result.Transformations {
				report.WriteString(fmt.Sprintf("**%s** (Line %d)\n", transform.Name, transform.LineNumber))
				report.WriteString(fmt.Sprintf("- Description: %s\n", transform.Description))
				report.WriteString(fmt.Sprintf("- Before: `%s`\n", transform.Before))
				report.WriteString(fmt.Sprintf("- After: `%s`\n\n", transform.After))
			}
		}

		if len(result.Warnings) > 0 {
			report.WriteString("### Warnings\n\n")
			for _, warning := range result.Warnings {
				report.WriteString(fmt.Sprintf("**Line %d**: %s\n", warning.LineNumber, warning.Message))
				if warning.Suggestion != "" {
					report.WriteString(fmt.Sprintf("- Suggestion: %s\n", warning.Suggestion))
				}
				report.WriteString("\n")
			}
		}
	}

	// Migration checklist
	report.WriteString("## Post-Migration Checklist\n\n")
	report.WriteString("- [ ] Review all transformed code for correctness\n")
	report.WriteString("- [ ] Update import statements as needed\n")
	report.WriteString("- [ ] Run tests to ensure functionality is preserved\n")
	report.WriteString("- [ ] Update Go version to 1.24+ if using generics\n")
	report.WriteString("- [ ] Consider enabling feature flags gradually\n")
	report.WriteString("- [ ] Review and address all warnings\n")
	report.WriteString("- [ ] Update documentation and examples\n\n")

	report.WriteString("For questions or issues, see: https://github.com/go-gorp/gorp/blob/v3/docs/migration.md\n")

	return report.String()
}

// SaveTransformedFile saves the transformed code to a file
func (mt *MigrationTransformer) SaveTransformedFile(result *TransformResult, outputPath string) error {
	return os.WriteFile(outputPath, []byte(result.TransformedCode), 0644)
}

// ValidateTransformation validates that the transformed code is syntactically correct
func (mt *MigrationTransformer) ValidateTransformation(result *TransformResult) error {
	_, err := parser.ParseFile(mt.fileSet, result.Filename, result.TransformedCode, parser.ParseComments)
	return err
}