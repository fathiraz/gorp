// Code generated by mockery v2.49.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// hasContext is an autogenerated mock type for the hasContext type
type hasContext struct {
	mock.Mock
}

// Context provides a mock function with given fields:
func (_m *hasContext) Context() context.Context {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Context")
	}

	var r0 context.Context
	if rf, ok := ret.Get(0).(func() context.Context); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(context.Context)
		}
	}

	return r0
}

// newHasContext creates a new instance of hasContext. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newHasContext(t interface {
	mock.TestingT
	Cleanup(func())
}) *hasContext {
	mock := &hasContext{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
