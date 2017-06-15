package pilosa

import (
	"context"
	"errors"
	"sync"

	"github.com/pilosa/pilosa/pql"
)

type NewPluginConstructor func(*Executor) Plugin
type Plugin interface {
	Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error)
	Reduce(ctx context.Context, prev, v interface{}) interface{}
}

type PluginRegistryEntry struct {
	constructor NewPluginConstructor
	callInfo    *PQLCallInfo
}

// PluginRegistry holds a lookup of plugin constructors.
type pluginRegistry struct {
	mutex   *sync.RWMutex
	entries map[string]*PluginRegistryEntry
}

// newPluginRegistry returns a new instance of PluginRegistry.
func newPluginRegistry() *pluginRegistry {
	return &pluginRegistry{
		mutex:   &sync.RWMutex{},
		entries: make(map[string]*PluginRegistryEntry),
	}
}

var (
	pr = newPluginRegistry()
)

// RegisterPlugin registers a plugin constructor with the registry.
// Returns an error if the plugin is already registered.
func RegisterPlugin(callInfo *PQLCallInfo, fn NewPluginConstructor) error {
	return pr.register(callInfo, fn)
}

func (r *pluginRegistry) register(callInfo *PQLCallInfo, fn NewPluginConstructor) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.entries[callInfo.Name] != nil {
		return errors.New("plugin already registered")
	}
	entry := &PluginRegistryEntry{
		constructor: fn,
		callInfo:    callInfo,
	}
	r.entries[callInfo.Name] = entry
	return nil
}

// NewPlugin instantiates an already loaded plugin.
func NewPlugin(call *pql.Call, e *Executor) (Plugin, error) {
	return pr.newPlugin(call, e)
}

func (r *pluginRegistry) newPlugin(call *pql.Call, e *Executor) (Plugin, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if entry, ok := r.entries[call.Name]; ok {
		return entry.constructor(e), nil
	}

	return nil, errors.New("plugin not found")
}
