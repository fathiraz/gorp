// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"context"
	"fmt"
)

// HookPoint represents when a hook should be executed
type HookPoint int

// Hook execution points
const (
	BeforeInsert HookPoint = iota
	AfterInsert
	BeforeUpdate
	AfterUpdate
	BeforeDelete
	AfterDelete
	AfterSelect
)

// HookError represents an error that occurred during hook execution
type HookError struct {
	Point  HookPoint
	Entity interface{}
	Err    error
}

func (e *HookError) Error() string {
	points := map[HookPoint]string{
		BeforeInsert: "before insert",
		AfterInsert:  "after insert",
		BeforeUpdate: "before update",
		AfterUpdate:  "after update",
		BeforeDelete: "before delete",
		AfterDelete:  "after delete",
		AfterSelect:  "after select",
	}
	return fmt.Sprintf("hook error at %s: %v", points[e.Point], e.Err)
}

// Hook represents a function that can be executed at various points
// during the lifecycle of an entity.
type Hook interface {
	Execute(ctx context.Context, executor SqlExecutor) error
}

// HookFunc is a function type that implements the Hook interface
type HookFunc func(ctx context.Context, executor SqlExecutor) error

// Execute implements the Hook interface
func (f HookFunc) Execute(ctx context.Context, executor SqlExecutor) error {
	return f(ctx, executor)
}

// HasPostGet provides PostGet() which will be executed after the GET statement.
//
// Example:
//
//	type User struct {
//	    ID      int64
//	    Name    string
//	    Profile *Profile
//	}
//
//	func (u *User) PostGet(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Load related profile after user is retrieved
//	    return exec.SelectOne(ctx, &u.Profile, "SELECT * FROM profiles WHERE user_id = ?", u.ID)
//	}
type HasPostGet interface {
	PostGet(context.Context, SqlExecutor) error
}

// HasPostDelete provides PostDelete() which will be executed after the DELETE statement.
//
// Example:
//
//	func (u *User) PostDelete(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Cleanup related data after user is deleted
//	    _, err := exec.Exec(ctx, "DELETE FROM user_logs WHERE user_id = ?", u.ID)
//	    return err
//	}
type HasPostDelete interface {
	PostDelete(context.Context, SqlExecutor) error
}

// HasPostUpdate provides PostUpdate() which will be executed after the UPDATE statement.
//
// Example:
//
//	func (u *User) PostUpdate(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Log update action
//	    return exec.Insert(ctx, &UserLog{UserID: u.ID, Action: "updated"})
//	}
type HasPostUpdate interface {
	PostUpdate(context.Context, SqlExecutor) error
}

// HasPostInsert provides PostInsert() which will be executed after the INSERT statement.
//
// Example:
//
//	func (u *User) PostInsert(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Initialize user settings after insert
//	    return exec.Insert(ctx, &UserSettings{UserID: u.ID})
//	}
type HasPostInsert interface {
	PostInsert(context.Context, SqlExecutor) error
}

// HasPreDelete provides PreDelete() which will be executed before the DELETE statement.
//
// Example:
//
//	func (u *User) PreDelete(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Validate delete operation
//	    var count int64
//	    err := exec.SelectOne(ctx, &count, "SELECT COUNT(*) FROM active_sessions WHERE user_id = ?", u.ID)
//	    if err != nil {
//	        return err
//	    }
//	    if count > 0 {
//	        return errors.New("cannot delete user with active sessions")
//	    }
//	    return nil
//	}
type HasPreDelete interface {
	PreDelete(context.Context, SqlExecutor) error
}

// HasPreUpdate provides PreUpdate() which will be executed before UPDATE statement.
//
// Example:
//
//	func (u *User) PreUpdate(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Validate update operation
//	    if u.Name == "" {
//	        return errors.New("user name cannot be empty")
//	    }
//	    return nil
//	}
type HasPreUpdate interface {
	PreUpdate(context.Context, SqlExecutor) error
}

// HasPreInsert provides PreInsert() which will be executed before INSERT statement.
//
// Example:
//
//	func (u *User) PreInsert(ctx context.Context, exec gorp.SqlExecutor) error {
//	    // Set default values before insert
//	    if u.CreatedAt.IsZero() {
//	        u.CreatedAt = time.Now()
//	    }
//	    return nil
//	}
type HasPreInsert interface {
	PreInsert(context.Context, SqlExecutor) error
}

// EntityWithHooks is a convenience interface that combines all hook interfaces
type EntityWithHooks interface {
	HasPreInsert
	HasPostInsert
	HasPreUpdate
	HasPostUpdate
	HasPreDelete
	HasPostDelete
	HasPostGet
}

// HookMap is a type-safe way to register hooks for specific entities
type HookMap[T any] struct {
	beforeInsert []Hook
	afterInsert  []Hook
	beforeUpdate []Hook
	afterUpdate  []Hook
	beforeDelete []Hook
	afterDelete  []Hook
	afterSelect  []Hook
}

// NewHookMap creates a new HookMap for a specific entity type
func NewHookMap[T any]() *HookMap[T] {
	return &HookMap[T]{
		beforeInsert: make([]Hook, 0),
		afterInsert:  make([]Hook, 0),
		beforeUpdate: make([]Hook, 0),
		afterUpdate:  make([]Hook, 0),
		beforeDelete: make([]Hook, 0),
		afterDelete:  make([]Hook, 0),
		afterSelect:  make([]Hook, 0),
	}
}

// AddHook adds a hook to be executed at the specified point
func (h *HookMap[T]) AddHook(point HookPoint, hook Hook) {
	switch point {
	case BeforeInsert:
		h.beforeInsert = append(h.beforeInsert, hook)
	case AfterInsert:
		h.afterInsert = append(h.afterInsert, hook)
	case BeforeUpdate:
		h.beforeUpdate = append(h.beforeUpdate, hook)
	case AfterUpdate:
		h.afterUpdate = append(h.afterUpdate, hook)
	case BeforeDelete:
		h.beforeDelete = append(h.beforeDelete, hook)
	case AfterDelete:
		h.afterDelete = append(h.afterDelete, hook)
	case AfterSelect:
		h.afterSelect = append(h.afterSelect, hook)
	}
}

// ExecuteHooks executes all hooks for a specific point
func (h *HookMap[T]) ExecuteHooks(ctx context.Context, point HookPoint, executor SqlExecutor) error {
	var hooks []Hook
	switch point {
	case BeforeInsert:
		hooks = h.beforeInsert
	case AfterInsert:
		hooks = h.afterInsert
	case BeforeUpdate:
		hooks = h.beforeUpdate
	case AfterUpdate:
		hooks = h.afterUpdate
	case BeforeDelete:
		hooks = h.beforeDelete
	case AfterDelete:
		hooks = h.afterDelete
	case AfterSelect:
		hooks = h.afterSelect
	}

	for _, hook := range hooks {
		if err := hook.Execute(ctx, executor); err != nil {
			return &HookError{
				Point:  point,
				Entity: new(T),
				Err:    err,
			}
		}
	}
	return nil
}
