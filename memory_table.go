package memorytable

import (
	"reflect"

	"github.com/pkg/errors"
)

var Error_RecordNotFound = errors.New("record not found")

type TableRows[T any] []T

func (records TableRows[T]) Uniqueue(keyFn func(row T) (key string)) []T {
	var result []T
	m := make(map[string]struct{})
	for _, v := range records {
		key := keyFn(v)
		if _, ok := m[key]; !ok {
			m[key] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

func (records TableRows[T]) GroupBy(groupValue func(row T) (key string)) map[string][]T {
	m := make(map[string][]T)
	for _, v := range records {
		groupVal := groupValue(v)
		if _, ok := m[groupVal]; !ok {
			m[groupVal] = make([]T, 0)
		}
		m[groupVal] = append(m[groupVal], v)
	}
	return m
}

func (records TableRows[T]) Contains(v T) bool {
	for _, v2 := range records {
		if reflect.DeepEqual(v, v2) {
			return true
		}
	}
	return false
}

func (records TableRows[T]) First() (first *T, exists bool) {
	if len(records) == 0 {
		return nil, false
	}
	return &records[0], true
}

func (records TableRows[T]) FirstWithDefault() (first T) {
	if len(records) == 0 {
		return *new(T)
	}
	return records[0]
}

func (records TableRows[T]) GetOne(fn func(row T) bool) (row *T, exists bool) {
	for _, r := range records {
		if fn(r) {
			return &r, true
		}
	}
	return nil, false
}
func (records TableRows[T]) GetOneWithError(fn func(row T) bool) (row *T, err error) {
	for _, r := range records {
		if fn(r) {
			return &r, nil
		}
	}
	err = Error_RecordNotFound
	return nil, err
}

func (records TableRows[T]) GetOneWithDefault(fn func(row T) bool) (row T) {
	for _, r := range records {
		if fn(r) {
			return r
		}
	}
	return *new(T)
}

func (records TableRows[T]) IsEmpty() (yes bool) {
	return len(records) == 0
}

func (records TableRows[T]) Filter(fn func(one T) bool) (sub []T) {
	sub = make([]T, 0)
	for _, v := range records {
		if fn(v) {
			sub = append(sub, v)
		}
	}
	return sub
}

func (records TableRows[T]) FilterEmpty() []T {
	return records.Filter(func(one T) bool {
		switch v := any(one).(type) {
		case string, *string:
			return v != ""
		case int, int32, int64, *int, *int64, *int32:
			return v != 0
		case []byte:
			return len(v) != 0
		case *[]byte:
			return len(*v) != 0
		default:
			rv := reflect.Indirect(reflect.ValueOf(one))
			if !rv.IsValid() || rv.IsNil() || rv.IsZero() {
				return false
			}
		}
		return true
	})
}

func (records TableRows[T]) Walk(fn func(one *T, index int) (err error)) (err error) {
	for i := 0; i < len(records); i++ {
		if err = fn(&records[i], i); err != nil {
			return err
		}
	}
	return nil
}

func (records TableRows[T]) Reverse(arr []T) (reversed []T) {
	reversed = make([]T, 0)
	for i := len(arr) - 1; i >= 0; i-- {
		reversed = append(reversed, arr[i])
	}
	return reversed

}

type RecordsColumn[T any, V any] []T

func (records RecordsColumn[T, V]) Column(fn func(row T) V) []V {
	var result []V
	for _, row := range records {
		result = append(result, fn(row))
	}
	return result
}

func (records RecordsColumn[T, V]) Map(fn func(one T) (value V)) (values []V) {
	values = make([]V, 0)
	for _, v := range records {
		values = append(values, fn(v))
	}
	return values
}
