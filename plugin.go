package pilosa

import (
	"context"
	"errors"
	"sync"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

type NewPluginConstructor func(*Executor) Plugin
type Plugin interface {
	Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error)
	Reduce(ctx context.Context, prev, v interface{}) interface{}
	Decode(*internal.QueryResult) (interface{}, error)
	Final() interface{}
}

// PluginRegistry holds a lookup of plugin constructors.
type pluginRegistry struct {
	mutex *sync.RWMutex
	fns   map[string]NewPluginConstructor
}

// newPluginRegistry returns a new instance of PluginRegistry.
func newPluginRegistry() *pluginRegistry {
	return &pluginRegistry{
		mutex: &sync.RWMutex{},
		fns:   make(map[string]NewPluginConstructor),
	}
}

var (
	pr = newPluginRegistry()
)

// RegisterPlugin registers a plugin constructor with the registry.
// Returns an error if the plugin is already registered.
func RegisterPlugin(name string, fn NewPluginConstructor) error {
	return pr.register(name, fn)
}

func (r *pluginRegistry) register(name string, fn NewPluginConstructor) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.fns[name] != nil {
		return errors.New("plugin already registered")
	}
	r.fns[name] = fn
	return nil
}

// NewPlugin instantiates an already loaded plugin.
func NewPlugin(name string, e *Executor) (Plugin, error) {
	return pr.newPlugin(name, e)
}

func (r *pluginRegistry) newPlugin(name string, e *Executor) (Plugin, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	fn := r.fns[name]
	if fn == nil {
		return nil, errors.New("plugin not found")
	}

	return fn(e), nil
}

func IsPlugin(name string) bool {
	return pr.isPlugin(name)
}

func (r *pluginRegistry) isPlugin(name string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	_, found := r.fns[name]
	return found

}
