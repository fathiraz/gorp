// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// BindingCache is a thread-safe cache for binding plans
type BindingCache struct {
	cache sync.Map // map[reflect.Type]*bindPlan
}

// Get returns a cached binding plan or creates a new one
func (c *BindingCache) Get(key reflect.Type, creator func() *bindPlan) *bindPlan {
	if plan, ok := c.cache.Load(key); ok {
		return plan.(*bindPlan)
	}
	plan := creator()
	actual, _ := c.cache.LoadOrStore(key, plan)
	return actual.(*bindPlan)
}

// CustomScanner binds a database column value to a Go type.
// It provides type-safe scanning of database values into Go types.
//
// Example:
//
//	type JSONField struct {
//	    Data map[string]interface{}
//	}
//
//	scanner := &CustomScanner{
//	    Holder: &[]byte{},
//	    Target: &field.Data,
//	    Binder: func(holder, target interface{}) error {
//	        b := holder.(*[]byte)
//	        return json.Unmarshal(*b, target)
//	    },
//	}
type CustomScanner struct {
	// After a row is scanned, Holder will contain the value from the database column.
	// Initialize the CustomScanner with the concrete Go type you wish the database
	// driver to scan the raw column into.
	Holder interface{}

	// Target typically holds a pointer to the target struct field to bind the Holder
	// value to.
	Target interface{}

	// Binder is a custom function that converts the holder value to the target type
	// and sets target accordingly. This function should return error if a problem
	// occurs converting the holder to the target.
	Binder func(holder interface{}, target interface{}) error
}

// ColumnFilter is used to filter columns when selectively updating.
// Return true to include the column in the update.
//
// Example:
//
//	// Only update non-empty string columns
//	filter := func(col *ColumnMap) bool {
//	    if v, ok := col.value.(string); ok {
//	        return v != ""
//	    }
//	    return true
//	}
type ColumnFilter func(*ColumnMap) bool

// acceptAllFilter is the default filter that includes all columns
func acceptAllFilter(col *ColumnMap) bool {
	return true
}

// Bind is called automatically by gorp after Scan()
func (cs CustomScanner) Bind() error {
	return cs.Binder(cs.Holder, cs.Target)
}

// bindPlan represents a cached query plan for a specific operation
type bindPlan struct {
	query             string
	argFields         []string
	keyFields         []string
	versField         string
	autoIncrIdx       int
	autoIncrFieldName string
	once              sync.Once
	prepared          atomic.Bool // Track if the plan has been prepared
}

// TypedBindPlan provides type-safe binding operations
type TypedBindPlan[T any] struct {
	*bindPlan
	conv TypeConverter
}

// NewTypedBindPlan creates a new type-safe binding plan
func NewTypedBindPlan[T any](plan *bindPlan, conv TypeConverter) *TypedBindPlan[T] {
	return &TypedBindPlan[T]{
		bindPlan: plan,
		conv:     conv,
	}
}

// bindInstance represents a single execution of a bind plan
type bindInstance struct {
	query             string
	args              []interface{}
	keys              []interface{}
	existingVersion   int64
	versField         string
	autoIncrIdx       int
	autoIncrFieldName string
}

// createBindInstance creates a new bind instance from the plan
func (plan *bindPlan) createBindInstance(elem reflect.Value, conv TypeConverter) (bindInstance, error) {
	bi := bindInstance{
		query:             plan.query,
		autoIncrIdx:       plan.autoIncrIdx,
		autoIncrFieldName: plan.autoIncrFieldName,
		versField:         plan.versField,
	}

	if plan.versField != "" {
		bi.existingVersion = elem.FieldByName(plan.versField).Int()
	}

	// Pre-allocate slices to avoid reallocations
	bi.args = make([]interface{}, 0, len(plan.argFields))
	bi.keys = make([]interface{}, 0, len(plan.keyFields))

	// Process argument fields
	if err := plan.processArgFields(elem, conv, &bi); err != nil {
		return bindInstance{}, err
	}

	// Process key fields
	if err := plan.processKeyFields(elem, conv, &bi); err != nil {
		return bindInstance{}, err
	}

	return bi, nil
}

// processArgFields processes argument fields for binding
func (plan *bindPlan) processArgFields(elem reflect.Value, conv TypeConverter, bi *bindInstance) error {
	for _, field := range plan.argFields {
		if field == versFieldConst {
			newVer := bi.existingVersion + 1
			bi.args = append(bi.args, newVer)
			if bi.existingVersion == 0 {
				elem.FieldByName(plan.versField).SetInt(int64(newVer))
			}
			continue
		}

		val := elem.FieldByName(field).Interface()
		if conv != nil {
			var err error
			val, err = conv.ToDb(val)
			if err != nil {
				return fmt.Errorf("error converting field %s: %w", field, err)
			}
		}
		bi.args = append(bi.args, val)
	}
	return nil
}

// processKeyFields processes key fields for binding
func (plan *bindPlan) processKeyFields(elem reflect.Value, conv TypeConverter, bi *bindInstance) error {
	for _, field := range plan.keyFields {
		val := elem.FieldByName(field).Interface()
		if conv != nil {
			var err error
			val, err = conv.ToDb(val)
			if err != nil {
				return fmt.Errorf("error converting key field %s: %w", field, err)
			}
		}
		bi.keys = append(bi.keys, val)
	}
	return nil
}

// CreateTypedInstance creates a type-safe bind instance
func (plan *TypedBindPlan[T]) CreateTypedInstance(elem T) (bindInstance, error) {
	return plan.createBindInstance(reflect.ValueOf(elem), plan.conv)
}

// bindInsert creates a bind instance for an insert
func (t *TableMap) bindInsert(elem reflect.Value) (bindInstance, error) {
	plan := &t.insertPlan
	plan.once.Do(func() {
		plan.autoIncrIdx = -1

		s := bytes.Buffer{}
		s2 := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("insert into %s (", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		x := 0
		first := true
		for y := range t.Columns {
			col := t.Columns[y]
			if !(col.isAutoIncr && t.dbmap.Dialect.AutoIncrBindValue() == "") {
				if !col.Transient {
					if !first {
						s.WriteString(",")
						s2.WriteString(",")
					}
					s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))

					if col.isAutoIncr {
						s2.WriteString(t.dbmap.Dialect.AutoIncrBindValue())
						plan.autoIncrIdx = y
						plan.autoIncrFieldName = col.fieldName
					} else {
						if col.DefaultValue == "" {
							s2.WriteString(t.dbmap.Dialect.BindVar(x))
							if col == t.version {
								plan.versField = col.fieldName
								plan.argFields = append(plan.argFields, versFieldConst)
							} else {
								plan.argFields = append(plan.argFields, col.fieldName)
							}
							x++
						} else {
							s2.WriteString(col.DefaultValue)
						}
					}
					first = false
				}
			} else {
				plan.autoIncrIdx = y
				plan.autoIncrFieldName = col.fieldName
			}
		}
		s.WriteString(") values (")
		s.WriteString(s2.String())
		s.WriteString(")")
		if plan.autoIncrIdx > -1 {
			s.WriteString(t.dbmap.Dialect.AutoIncrInsertSuffix(t.Columns[plan.autoIncrIdx]))
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

// bindUpdate creates a bind instance for an update
func (t *TableMap) bindUpdate(elem reflect.Value, colFilter ColumnFilter) (bindInstance, error) {
	if colFilter == nil {
		colFilter = acceptAllFilter
	}

	plan := &t.updatePlan
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("update %s set ", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))
		x := 0

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.isAutoIncr && !col.Transient && colFilter(col) {
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				s.WriteString("=")
				s.WriteString(t.dbmap.Dialect.BindVar(x))

				if col == t.version {
					plan.versField = col.fieldName
					plan.argFields = append(plan.argFields, versFieldConst)
				} else {
					plan.argFields = append(plan.argFields, col.fieldName)
				}
				x++
			}
		}

		s.WriteString(" where ")
		for y := range t.keys {
			col := t.keys[y]
			if y > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.argFields = append(plan.argFields, col.fieldName)
			plan.keyFields = append(plan.keyFields, col.fieldName)
			x++
		}
		if plan.versField != "" {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))
			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

// bindDelete creates a bind instance for a delete
func (t *TableMap) bindDelete(elem reflect.Value) (bindInstance, error) {
	plan := &t.deletePlan
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("delete from %s", t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.Transient {
				if col == t.version {
					plan.versField = col.fieldName
				}
			}
		}

		s.WriteString(" where ")
		for x := range t.keys {
			k := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(k.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, k.fieldName)
			plan.argFields = append(plan.argFields, k.fieldName)
		}
		if plan.versField != "" {
			s.WriteString(" and ")
			s.WriteString(t.dbmap.Dialect.QuoteField(t.version.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(len(plan.argFields)))

			plan.argFields = append(plan.argFields, plan.versField)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan.createBindInstance(elem, t.dbmap.TypeConverter)
}

// bindGet creates a bind instance for a get
func (t *TableMap) bindGet() *bindPlan {
	plan := &t.getPlan
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString("select ")

		x := 0
		for _, col := range t.Columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
				plan.argFields = append(plan.argFields, col.fieldName)
				x++
			}
		}
		s.WriteString(" from ")
		s.WriteString(t.dbmap.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName))
		s.WriteString(" where ")
		for x := range t.keys {
			col := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbmap.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbmap.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, col.fieldName)
		}
		s.WriteString(t.dbmap.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan
}
