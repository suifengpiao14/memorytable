package memorytable

import (
	"encoding/json"
	"reflect"
	"slices"

	"github.com/pkg/errors"
)

var Error_RecordNotFound = errors.New("record not found")

type Table[T any] []T

// Deprecated: 请使用 NewTable
func NewTableRows[T any](records ...T) Table[T] {
	return records
}

func NewTable[T any](records ...T) Table[T] {
	return records
}

func (records Table[T]) Count() int {
	return len(records)
}

func (records Table[T]) ToSlice() []T {
	return records
}

// Set 存在则更新，不存在则插入
func (records Table[T]) Set(identityFn func(t T) (identity string), moreTableRows ...T) (merged Table[T]) {
	m := records.Map(identityFn)
	for _, v := range moreTableRows {
		key := identityFn(v)
		m[key] = v
	}
	merged = make([]T, len(m))
	i := 0
	for _, v := range m {
		merged[i] = v
		i++
	}
	return merged
}

// Insert 批量生成记录
func (records Table[T]) Insert(row T, index int) (initedRows Table[T]) {

	// 确保index是有效的
	if index <= 0 {
		records = append([]T{row}, records...)
		return records
	}
	if index >= len(records) {
		records = append(records, row)
		return records
	}
	// 将切片分为两部分，插入a到index的位置
	befor := records[:index]
	after := records[index:]
	befor = append(befor, row)
	befor = append(befor, after...)
	records = befor
	return records
}

// Update  覆盖记录，如果存在则用新的值替换旧的。可以和Insert 结合使用，快速从全局数据中筛选部分子数据，同时保留不在全局数据内的记录。(如权限包中根据总权限回复指定部分权限数据，并保证全部记录能回复)
func (records Table[T]) Update(identityFn func(t T) (identity string), valueTableRows ...T) Table[T] {
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
func (records Table[T]) Intersection(seconds Table[T], identityFn func(row T) string) Table[T] {
	secondMap := seconds.Map(identityFn)
	result := make([]T, 0)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; ok {
			result = append(result, v)
		}
	}
	return result
}
func (records Table[T]) Diff(subtrahend Table[T], identityFn func(row T) string) Table[T] {
	secondMap := make(map[string]struct{})
	for _, v := range subtrahend {
		key := identityFn(v)
		secondMap[key] = struct{}{}
	}
	result := make([]T, 0)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; !ok {
			result = append(result, v)
		}
	}
	return result
}
func (records Table[T]) Index(identityFn func(row T) string) map[string]T {
	m := make(map[string]T)
	for _, v := range records {
		m[identityFn(v)] = v
	}
	return m
}

// Deprecated: 请使用 Index
func (records Table[T]) Map(identityFn func(row T) string) map[string]T {
	return records.Index(identityFn)
}
func (records Table[T]) HasDiff(subtrahend Table[T], identityFn func(row T) string) bool {
	secondMap := subtrahend.Map(identityFn)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; !ok {
			return true
		}
	}
	return false
}
func (records Table[T]) HasIntersection(seconds Table[T], identityFn func(row T) string) bool {
	secondMap := seconds.Map(identityFn)
	for _, v := range records {
		key := identityFn(v)
		if _, ok := secondMap[key]; ok {
			return true
		}
	}
	return false
}

// IsSubsetTo 判断records是否为fullSet的子集 ,增加是否为空集返回，方便提示使用者考虑空集情况
func (records Table[T]) IsSubsetTo(fullSet Table[T], identityFn func(row T) string) (isSubSet bool, isEmptySet bool) {
	if len(records) == 0 {
		return true, true
	}
	inter := records.Intersection(fullSet, identityFn)
	ok := len(inter) == len(records)
	return ok, false
}

// IsSubset 判断subSet是否为records的子集 ,增加是否为空集返回，提示使用者考虑空集情况
func (records Table[T]) IsSubset(subSet Table[T], identityFn func(row T) string) (isSubSet bool, isEmptySet bool) {
	if len(subSet) == 0 {
		return true, true
	}
	inter := records.Intersection(subSet, identityFn)
	ok := len(inter) == len(subSet)
	return ok, false
}
func (records Table[T]) Uniqueue(keyFn func(row T) (key string)) Table[T] {
	result := make([]T, 0)
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
func (records Table[T]) Sum(sumFn func(row T) (number int64)) (sum int64) {
	for _, v := range records {
		sum += sumFn(v)
	}
	return sum
}
func (records Table[T]) Json() (s string, err error) {
	b, err := json.Marshal(records)
	if err != nil {
		return "", err
	}
	s = string(b)
	return s, nil
}

func (records Table[T]) JsonMust() (s string) {
	b, err := json.Marshal(records)
	if err != nil {
		err = errors.WithMessagef(err, "json marshal error  TableRows[T]) JsonMust()")
		panic(err)
	}
	s = string(b)
	return s
}

// GroupBy 分组，返回map[string][]T 这里返回map[string]Table[T]不实用,实际场景多需要map[string][]T 格式，另外 map[string][]T 格式遍历时很方便转为 Table[T] (其它函数返回 Table[T] 是方便链式调用，该函数不能参与链式调用)
func (records Table[T]) GroupBy(groupValue func(row T) (key string)) map[string][]T {
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

func (records Table[T]) OrderBy(orderBy func(a, b T) (order int)) Table[T] {
	slices.SortFunc(records, orderBy)
	return records
}

func (records Table[T]) Contains(v T) bool {
	for _, v2 := range records {
		if reflect.DeepEqual(v, v2) {
			return true
		}
	}
	return false
}

func (records Table[T]) ContainsFunc(comparedFn func(one T) bool) bool {
	return slices.ContainsFunc(records, comparedFn)

}

// Deprecated: use ContainsFunc instead
func (records Table[T]) ContainsWithFunc(comparedFn func(one T) bool) bool {
	// for _, v := range records {
	// 	if comparedFn(v) {
	// 		return true
	// 	}
	// }
	return records.ContainsFunc(comparedFn)
}

func (records Table[T]) First() (first *T, exists bool) {
	if len(records) == 0 {
		return nil, false
	}
	return &records[0], true
}

func (records Table[T]) FirstWithDefault() (first T) {
	if len(records) == 0 {
		return *new(T)
	}
	return records[0]
}

func (records Table[T]) GetOne(fn func(row T) bool) (row *T, exists bool) {
	for _, r := range records {
		if fn(r) {
			return &r, true
		}
	}
	return nil, false
}
func (records Table[T]) GetOneWithError(fn func(row T) bool) (row *T, err error) {
	for _, r := range records {
		if fn(r) {
			return &r, nil
		}
	}
	err = Error_RecordNotFound
	return nil, err
}

func (records Table[T]) GetOneWithDefault(fn func(row T) bool) (row T) {
	for _, r := range records {
		if fn(r) {
			return r
		}
	}
	return *new(T)
}

func (records Table[T]) IsEmpty() (yes bool) {
	return len(records) == 0
}

func (records Table[T]) Where(fn func(record T) bool) (sub Table[T]) {
	sub = make([]T, 0)
	for _, v := range records {
		if fn(v) {
			sub = append(sub, v)
		}
	}
	return sub
}

// Deprecated: Use Where instead.
func (records Table[T]) Filter(fn func(record T) bool) (sub Table[T]) {
	return records.Where(fn)
}

func (records Table[T]) FilterEmpty() Table[T] {
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

func (records Table[T]) Walk(fn func(one *T, index int) (err error)) (err error) {
	for i := 0; i < len(records); i++ {
		if err = fn(&records[i], i); err != nil {
			return err
		}
	}
	return nil
}

func (records Table[T]) Reverse(arr []T) (reversed Table[T]) {
	reversed = make([]T, 0)
	for i := len(arr) - 1; i >= 0; i-- {
		reversed = append(reversed, arr[i])
	}
	return reversed

}

type RecordsColumn[T any, V any] []T

func (records RecordsColumn[T, V]) Column(fn func(row T) V) []V {
	result := make([]V, 0)
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
	result := make([]V, 0)
	for _, row := range records {
		result = append(result, fn(row))
	}
	return result
}
func Map[T, V any](records []T, fn func(row T) (value V)) []V {
	result := make([]V, 0)
	for _, row := range records {
		result = append(result, fn(row))
	}
	return result
}
