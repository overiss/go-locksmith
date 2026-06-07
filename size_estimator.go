package locksmith

import "reflect"

func autoSizeEstimator[K Key, V any](key K, value V) int {
	return estimateAny(reflect.ValueOf(key), 0, nil) + estimateAny(reflect.ValueOf(value), 0, nil)
}

type visit struct {
	ptr uintptr
	typ reflect.Type
}

func estimateAny(v reflect.Value, depth int, seen map[visit]struct{}) int {
	if !v.IsValid() {
		return 0
	}
	if depth > 32 {
		return int(v.Type().Size())
	}
	if seen == nil {
		seen = make(map[visit]struct{}, 8)
	}

	t := v.Type()
	size := int(t.Size())

	switch v.Kind() {
	case reflect.String:
		return size + v.Len()
	case reflect.Pointer:
		if v.IsNil() {
			return size
		}
		key := visit{ptr: v.Pointer(), typ: t}
		if _, ok := seen[key]; ok {
			return size
		}
		seen[key] = struct{}{}
		return size + estimateAny(v.Elem(), depth+1, seen)
	case reflect.Interface:
		if v.IsNil() {
			return size
		}
		return size + estimateAny(v.Elem(), depth+1, seen)
	case reflect.Slice:
		if v.IsNil() {
			return size
		}
		dataPtr := v.Pointer()
		key := visit{ptr: dataPtr, typ: t}
		if dataPtr != 0 {
			if _, ok := seen[key]; ok {
				return size
			}
			seen[key] = struct{}{}
		}
		extra := v.Cap() * int(t.Elem().Size())
		for i := 0; i < v.Len(); i++ {
			extra += estimateExtra(v.Index(i), depth+1, seen)
		}
		return size + extra
	case reflect.Array:
		extra := 0
		for i := 0; i < v.Len(); i++ {
			extra += estimateExtra(v.Index(i), depth+1, seen)
		}
		return size + extra
	case reflect.Struct:
		extra := 0
		for i := 0; i < v.NumField(); i++ {
			extra += estimateExtra(v.Field(i), depth+1, seen)
		}
		return size + extra
	case reflect.Map:
		if v.IsNil() {
			return size
		}
		dataPtr := v.Pointer()
		key := visit{ptr: dataPtr, typ: t}
		if dataPtr != 0 {
			if _, ok := seen[key]; ok {
				return size
			}
			seen[key] = struct{}{}
		}

		extra := 0
		iter := v.MapRange()
		for iter.Next() {
			extra += estimateAny(iter.Key(), depth+1, seen)
			extra += estimateAny(iter.Value(), depth+1, seen)
			extra += 8 // rough per-entry map overhead
		}
		return size + extra
	default:
		return size
	}
}

func estimateExtra(v reflect.Value, depth int, seen map[visit]struct{}) int {
	if !v.IsValid() {
		return 0
	}

	switch v.Kind() {
	case reflect.String:
		return v.Len()
	case reflect.Pointer:
		if v.IsNil() {
			return 0
		}
		return estimateAny(v.Elem(), depth+1, seen)
	case reflect.Interface:
		if v.IsNil() {
			return 0
		}
		return estimateAny(v.Elem(), depth+1, seen)
	case reflect.Slice:
		if v.IsNil() {
			return 0
		}
		// Include full backing array estimate for referenced slice.
		return estimateAny(v, depth+1, seen)
	case reflect.Map:
		if v.IsNil() {
			return 0
		}
		return estimateAny(v, depth+1, seen)
	case reflect.Array:
		total := 0
		for i := 0; i < v.Len(); i++ {
			total += estimateExtra(v.Index(i), depth+1, seen)
		}
		return total
	case reflect.Struct:
		total := 0
		for i := 0; i < v.NumField(); i++ {
			total += estimateExtra(v.Field(i), depth+1, seen)
		}
		return total
	default:
		return 0
	}
}
