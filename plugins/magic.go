package plugins

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Magic", NewMagicPlugin)
}

// LoadPlugin represents a plugin that will read bitmaps from a register
type MagicPlugin struct {
	executor *pilosa.Executor
}

// NewMagicPlugin returns a new instance of MagicPlugin.
func NewMagicPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &MagicPlugin{e}
}

func getFragments(index string, frames []string, slice uint64, e *pilosa.Executor) (map[string]*pilosa.Fragment, error) {
	results := make(map[string]*pilosa.Fragment)
	for i, frame := range frames {
		view := e.Holder.View(index, frame, pilosa.ViewStandard)
		if view == nil {
			return nil, errors.New("Bad View")
		}
		f := view.Fragment(slice)
		results[frames[i]] = f
	}
	return results, nil
}

func makeFilter(rows, frames []string, fragments map[string]*pilosa.Fragment, cacheKey string, cache *pilosa.Bitmap) (*pilosa.Bitmap, string, *pilosa.Bitmap, error) {
	bitmaps := make([]*pilosa.Bitmap, len(rows))
	key := ""
	for i, row := range rows {
		f := fragments[frames[i]]
		id, err := strconv.ParseUint(row, 10, 64)
		if err != nil {
			return nil, "", nil, err
		}
		bm := f.Row(id)
		bitmaps[i] = bm
		if i < len(rows)-1 {
			key += row + ":"
		}

	}
	if cacheKey != key {
		cache = pilosa.NewBitmap()
		for i, bitmap := range bitmaps[:len(bitmaps)-2] {
			if i == 0 {
				cache = cache.Union(bitmap)
			} else {
				cache = cache.Intersect(bitmap)
			}

		}
	}

	next := bitmaps[len(bitmaps)-1]
	result := cache.Intersect(next)
	return result, key, cache, nil
}

// Map executes the plugin against a single slice.
func (p *MagicPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	var (
		groups             []interface{}
		frameArg, fieldArg string
	)

	args := call.Args
	if tmp, found := args["groups"]; found {
		groups = tmp.([]interface{})
	} else {
		return nil, errors.New("no groups required")
	}

	if tmp, found := args["frame_def"]; found {
		frameArg = tmp.(string)
	} else {
		return nil, errors.New("no frame definition required")
	}
	if tmp, found := args["sum_def"]; found {
		fieldArg = tmp.(string)
	} else {
		return nil, errors.New("no frame definition required")
	}

	fieldParts := strings.Split(fieldArg, " ")
	if len(fieldParts) != 2 {
		return nil, errors.New("invalid field def")
	}

	results := make(map[string]pilosa.SumCount)

	frames := strings.Split(frameArg, " ")
	fragments, err := getFragments(index, frames, slice, p.executor)
	if err != nil {
		return results, err
	}

	frame := p.executor.Holder.Frame(index, fieldParts[0])
	if frame == nil {
		return results, err
	}

	field := frame.Field(fieldParts[1])
	if field == nil {
		return results, err
	}

	sumFragment := p.executor.Holder.Fragment(index, fieldParts[0], pilosa.ViewFieldPrefix+fieldParts[1], slice)
	if sumFragment == nil {
		return results, err
	}

	var (
		cacheKey      string
		cache, filter *pilosa.Bitmap
	)
	for _, group := range groups {
		parts := strings.Split(group.(string), " ")
		filter, cacheKey, cache, err = makeFilter(parts, frames, fragments, cacheKey, cache)
		if err != nil {
			return results, err

		}
		sum, _, err := sumFragment.FieldSum(filter, field.BitDepth())
		if err != nil {
			return results, err

		}
		results[group.(string)] = pilosa.SumCount{
			Sum:   int64(sum),
			Count: int64(filter.Count()),
		}

	}

	return results, nil

}

///func merge(a, b map[string]pilosa.SumCount) map[string]pilosa.SumCount {
func merge(a, b map[string]pilosa.SumCount) map[string]pilosa.SumCount {
	for k, v := range b {
		s, present := a[k]
		if present {
			v.Sum += s.Sum
			v.Count += s.Count
		}
		a[k] = v
	}
	return a
}

// Reduce combines previous map results into a single value.
func (p *MagicPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]pilosa.SumCount:
		if prev != nil {
			p := prev.(map[string]pilosa.SumCount)
			return merge(p, x)

		}
		return x
	default:
		fmt.Printf("didn %T %v", v, v)
	}

	return v
}

func (p *MagicPlugin) Decode(response *internal.QueryResult) (interface{}, error) {
	if response != nil {
		return response.SumCount, nil
	}
	return nil, nil

}

func (p *MagicPlugin) Final() interface{} {
	return nil
}
