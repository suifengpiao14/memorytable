package memorytable

import (
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
)

var Error_RecordNotFound = errors.New("record not found")

type TableRows[T any] []T

func NewTableRows[T any](records ...T) TableRows[T] {
	return records
}

func (records TableRows[T]) Len() int {
	return len(records)
}

func (records TableRows[T]) ToSlice() []T {
	return records
}

// Set 存在则更新，不存在则插入
func (records TableRows[T]) Set(identityFn func(t T) (identity string), moreTableRows ...TableRows[T]) (merged TableRows[T]) {
	m := records.Map(identityFn)
	for _, more := range moreTableRows {
		for _, v := range more {
			key := identityFn(v)
			m[key] = v
		}
	}
	merged = make([]T, 0, len(m))
	i := 0
	for _, v := range m {
		merged[i] = v
		i++
	}
	return records
}

// Insert 批量生成记录
func (records TableRows[T]) Insert(identities []string, initFn func(identity string) (record T)) (initedRows TableRows[T]) {
	initedRows = Map(identities, func(identity string) (record T) {
		return initFn(identity)
	})
	return initedRows
}

// Update  覆盖记录，如果存在则用新的值替换旧的。可以和Insert 结合使用，快速从全局数据中筛选部分子数据，同时保留不在全局数据内的记录。(如权限包中根据总权限回复指定部分权限数据，并保证全部记录能回复)
func (records TableRows[T]) Update(identityFn func(t T) (identity string), valueTableRows ...T) TableRows[T] {
	m := make(map[string]T)
	for _, v := range valueTableRows {
		m[identityFn(v)] = v
	}

	for i := range records {
		identity := identityFn(records[i])
		if v, ok := m[identity]; ok {
			records[i] = v
		}

	}
	return records
}

// Intersection 返回两个集合的交集
func (records TableRows[T]) Intersection(seconds TableRows[T], identityFn func(row T) string) TableRows[T] {
	secondMap := seconds.Map(identityFn)
	var result []T
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; ok {
			result = append(result, v)
		}
	}
	return result
}
func (records TableRows[T]) Diff(subtrahend TableRows[T], identityFn func(row T) string) TableRows[T] {
	secondMap := make(map[string]struct{})
	for _, v := range subtrahend {
		key := identityFn(v)
		secondMap[key] = struct{}{}
	}
	var result []T
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; !ok {
			result = append(result, v)
		}
	}
	return result
}
func (records TableRows[T]) Map(identityFn func(row T) string) map[string]T {
	m := make(map[string]T)
	for _, v := range records {
		m[identityFn(v)] = v
	}
	return m
}
func (records TableRows[T]) HasDiff(subtrahend TableRows[T], identityFn func(row T) string) bool {
	secondMap := subtrahend.Map(identityFn)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; !ok {
			return true
		}
	}
	return false
}
func (records TableRows[T]) HasIntersection(seconds TableRows[T], identityFn func(row T) string) bool {
	secondMap := seconds.Map(identityFn)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; ok {
			return true
		}
	}
	return false
}

// IsSubsetTo 判断records是否为fullSet的子集
func (records TableRows[T]) IsSubsetTo(fullSet TableRows[T], identityFn func(row T) string) bool {
	inter := records.Intersection(fullSet, identityFn)
	ok := len(inter) == len(records)
	return ok
}

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
func (records TableRows[T]) Sum(sumFn func(row T) (number int64)) (sum int64) {
	for _, v := range records {
		sum += sumFn(v)
	}
	return sum
}
func (records TableRows[T]) Json() (s string, err error) {
	b, err := json.Marshal(records)
	if err != nil {
		return "", err
	}
	s = string(b)
	return s, nil
}

func (records TableRows[T]) JsonMust() (s string) {
	b, err := json.Marshal(records)
	if err != nil {
		err = errors.WithMessagef(err, "json marshal error  TableRows[T]) JsonMust()")
		panic(err)
	}
	s = string(b)
	return s
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
func (records TableRows[T]) ContainsWithFunc(comparedFn func(one T) bool) bool {
	for _, v := range records {
		if comparedFn(v) {
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

func Column[T, V any](records []T, fn func(row T) (value V)) []V {
	var result []V
	for _, row := range records {
		result = append(result, fn(row))
	}
	return result
}
func Map[T, V any](records []T, fn func(row T) (value V)) []V {
	var result []V
	for _, row := range records {
		result = append(result, fn(row))
	}
	return result
}
