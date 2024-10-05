package libstore

import (
	"errors"
)

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
	ErrLocation
	ErrKey
	ErrEntry
	ErrOpsInternal
	ErrKeyNotFound
)

type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

func (e *Error) Error() string {
	return e.Message
}

func NewError(err error) *Error {
	switch err.(type) {
	case LocationError:
		return &Error{Code: ErrLocation, Message: err.Error()}
	case KeyError:
		return &Error{Code: ErrKey, Message: err.Error()}
	case EntryError:
		return &Error{Code: ErrEntry, Message: err.Error()}
	case OpsInternalError:
		return &Error{Code: ErrOpsInternal, Message: err.Error()}
	case KeyNotFoundError:
		return &Error{Code: ErrKeyNotFound, Message: err.Error()}
	default:
		return &Error{Code: ErrUnknown, Message: "unknown error"}
	}
}

func TranslateToError(code int, message string) error {
	switch code {
	case 1:
		return LocationError(message)
	case 2:
		return KeyError(message)
	case 3:
		return EntryError(message)
	case 4:
		return OpsInternalError(message)
	case 5:
		return KeyNotFoundError(message)
	default:
		return errors.New(message)
	}
}
