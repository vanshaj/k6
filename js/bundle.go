/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package js

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/dop251/goja"
	"github.com/dop251/goja/unistring"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"gopkg.in/guregu/null.v3"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/compiler"
	"go.k6.io/k6/js/eventloop"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/lib"
	"go.k6.io/k6/lib/consts"
	"go.k6.io/k6/loader"
	"go.k6.io/k6/metrics"
)

// A Bundle is a self-contained bundle of scripts and resources.
// You can use this to produce identical BundleInstance objects.
type Bundle struct {
	Filename *url.URL
	Source   string
	Module   goja.ModuleRecord
	Program  *goja.Program
	Options  lib.Options

	BaseInitContext *InitContext

	RuntimeOptions    lib.RuntimeOptions
	CompatibilityMode lib.CompatibilityMode // parsed value
	registry          *metrics.Registry

	exports map[string]goja.Callable
}

// A BundleInstance is a self-contained instance of a Bundle.
type BundleInstance struct {
	Runtime        *goja.Runtime
	Context        *context.Context
	ModuleInstance goja.ModuleInstance

	// TODO: maybe just have a reference to the Bundle? or save and pass rtOpts?
	env map[string]string

	exports map[string]goja.Callable
}
type moduleCacheElement struct {
	err error
	m   goja.ModuleRecord
}

// NewBundle creates a new bundle from a source file and a filesystem.
func NewBundle(
	logger logrus.FieldLogger, src *loader.SourceData, filesystems map[string]afero.Fs, rtOpts lib.RuntimeOptions,
	registry *metrics.Registry,
) (*Bundle, error) {
	compatMode, err := lib.ValidateCompatibilityMode(rtOpts.CompatibilityMode.String)
	if err != nil {
		return nil, err
	}

	// Compile sources, both ES5 and ES6 are supported.
	code := string(src.Data)
	c := compiler.New(logger)
	c.Options = compiler.Options{
		CompatibilityMode: compatMode,
		Strict:            true,
		SourceMapLoader:   generateSourceMapLoader(logger, filesystems),
	}
	pwd := loader.Dir(src.URL)
	rt := goja.New()
	i := NewInitContext(logger, rt, c, compatMode, new(context.Context),
		filesystems, loader.Dir(src.URL))

	cache := make(map[string]moduleCacheElement)
	var resolveModule goja.HostResolveImportedModuleFunc
	resolveModule = func(_ interface{}, specifier string) (goja.ModuleRecord, error) {
		// TODO fix
		fileURL, err := loader.Resolve(pwd, specifier)
		if err != nil {
			return nil, err
		}
		if specifier == "k6" || strings.HasPrefix(specifier, "k6/") {
			mod, ok := i.modules[specifier]
			if !ok {
				return nil, fmt.Errorf("unknown module: %s", specifier)
			}
			k6m, ok := mod.(modules.Module)
			if !ok {
				return nil, fmt.Errorf("Unsupported not native module " + specifier)
			}
			return wrapModule(k6m), nil
		}
		originalSpecifier := specifier
		specifier = fileURL.String()
		k, ok := cache[specifier]
		if ok {
			return k.m, k.err
		}
		var resolvedSrc *loader.SourceData
		if originalSpecifier == src.URL.String() {
			// This mostly exists for tests ... kind of
			resolvedSrc = src
		} else {
			resolvedSrc, err = loader.Load(logger, filesystems, fileURL, originalSpecifier)
			if err != nil {
				return nil, err
			}
		}

		ast, isModule, err := c.Parse(string(resolvedSrc.Data), specifier, true)
		if err != nil {
			cache[specifier] = moduleCacheElement{err: err}
			return nil, err
		}
		if !isModule {
			logger.WithField("specifier", specifier).Error("Not module !!!!!!! TO BE IMPLEMENTED")
			return nil, fmt.Errorf("NOT MODULE TO BE IMPLEMENTED")
			// TODO warning
			// TODO Implement wrapper
		}
		m, err := goja.ModuleFromAST(specifier, ast, resolveModule)
		if err != nil {
			cache[specifier] = moduleCacheElement{err: err}
			return nil, err
		}
		cache[specifier] = moduleCacheElement{m: m}
		return m, nil
	}

	m, err := resolveModule(nil, src.URL.String())
	if err != nil {
		return nil, err
	}
	err = m.Link()
	if err != nil {
		return nil, err
	}
	// Make a bundle, instantiate it into a throwaway VM to populate caches.
	bundle := Bundle{
		Filename: src.URL,
		Source:   code,
		// Program:  pgm,
		Module:          m,
		BaseInitContext: i, RuntimeOptions: rtOpts,
		CompatibilityMode: compatMode,
		exports:           make(map[string]goja.Callable),
		registry:          registry,
	}
	if _, err = bundle.instantiate(logger, rt, bundle.BaseInitContext, 0); err != nil {
		return nil, err
	}

	err = bundle.getExports(logger, rt, true)
	if err != nil {
		return nil, err
	}

	return &bundle, nil
}

// NewBundleFromArchive creates a new bundle from an lib.Archive.
func NewBundleFromArchive(
	logger logrus.FieldLogger, arc *lib.Archive, rtOpts lib.RuntimeOptions, registry *metrics.Registry,
) (*Bundle, error) {
	if arc.Type != "js" {
		return nil, fmt.Errorf("expected bundle type 'js', got '%s'", arc.Type)
	}

	if !rtOpts.CompatibilityMode.Valid {
		// `k6 run --compatibility-mode=whatever archive.tar` should override
		// whatever value is in the archive
		rtOpts.CompatibilityMode = null.StringFrom(arc.CompatibilityMode)
	}
	compatMode, err := lib.ValidateCompatibilityMode(rtOpts.CompatibilityMode.String)
	if err != nil {
		return nil, err
	}

	c := compiler.New(logger)
	c.Options = compiler.Options{
		Strict:            true,
		CompatibilityMode: compatMode,
		SourceMapLoader:   generateSourceMapLoader(logger, arc.Filesystems),
	}
	pwd := loader.Dir(arc.PwdURL)
	rt := goja.New()
	i := NewInitContext(logger, rt, c, compatMode,
		new(context.Context), arc.Filesystems, arc.PwdURL)

	cache := make(map[string]moduleCacheElement)
	var resolveModule goja.HostResolveImportedModuleFunc
	resolveModule = func(_ interface{}, specifier string) (goja.ModuleRecord, error) {
		// TODO fix
		fileURL, err := loader.Resolve(pwd, specifier)
		if err != nil {
			return nil, err
		}
		if specifier == "k6" || strings.HasPrefix(specifier, "k6/") {
			mod, ok := i.modules[specifier]
			if !ok {
				return nil, fmt.Errorf("unknown module: %s", specifier)
			}
			k6m, ok := mod.(modules.Module)
			if !ok {
				return nil, fmt.Errorf("Unsupported not native module " + specifier)
			}
			return wrapModule(k6m), nil
		}
		originalSpecifier := specifier
		specifier = fileURL.String()
		k, ok := cache[specifier]
		if ok {
			return k.m, k.err
		}
		var resolvedSrc *loader.SourceData
		var data []byte
		if originalSpecifier == arc.Filename {
			// This mostly exists for tests ... kind of
			data = arc.Data
		} else {
			resolvedSrc, err = loader.Load(logger, arc.Filesystems, fileURL, originalSpecifier)
			if err != nil {
				return nil, err
			}
			data = resolvedSrc.Data
		}

		ast, isModule, err := c.Parse(string(data), specifier, true)
		if err != nil {
			cache[specifier] = moduleCacheElement{err: err}
			return nil, err
		}
		if !isModule {
			logger.WithField("specifier", specifier).Error("Not module !!!!!!! TO BE IMPLEMENTED")
			return nil, fmt.Errorf("NOT MODULE TO BE IMPLEMENTED")
			// TODO warning
			// TODO Implement wrapper
		}
		m, err := goja.ModuleFromAST(specifier, ast, resolveModule)
		if err != nil {
			cache[specifier] = moduleCacheElement{err: err}
			return nil, err
		}
		cache[specifier] = moduleCacheElement{m: m}
		return m, nil
	}

	m, err := resolveModule(nil, arc.Filename)
	if err != nil {
		return nil, err
	}
	err = m.Link()
	if err != nil {
		return nil, err
	}

	env := arc.Env
	if env == nil {
		// Older archives (<=0.20.0) don't have an "env" property
		env = make(map[string]string)
	}
	for k, v := range rtOpts.Env {
		env[k] = v
	}
	rtOpts.Env = env

	bundle := &Bundle{
		Filename: arc.FilenameURL,
		Source:   string(arc.Data),
		Module:   m,
		// Program:           pgm,
		Options:           arc.Options,
		BaseInitContext:   i,
		RuntimeOptions:    rtOpts,
		CompatibilityMode: compatMode,
		exports:           make(map[string]goja.Callable),
		registry:          registry,
	}

	if _, err = bundle.instantiate(logger, rt, bundle.BaseInitContext, 0); err != nil {
		return nil, err
	}

	// Grab exported objects, but avoid overwriting options, which would
	// be initialized from the metadata.json at this point.
	err = bundle.getExports(logger, rt, false)
	if err != nil {
		return nil, err
	}

	return bundle, nil
}

func (b *Bundle) makeArchive() *lib.Archive {
	arc := &lib.Archive{
		Type:              "js",
		Filesystems:       b.BaseInitContext.filesystems,
		Options:           b.Options,
		FilenameURL:       b.Filename,
		Data:              []byte(b.Source),
		PwdURL:            b.BaseInitContext.pwd,
		Env:               make(map[string]string, len(b.RuntimeOptions.Env)),
		CompatibilityMode: b.CompatibilityMode.String(),
		K6Version:         consts.Version,
		Goos:              runtime.GOOS,
	}
	// Copy env so changes in the archive are not reflected in the source Bundle
	for k, v := range b.RuntimeOptions.Env {
		arc.Env[k] = v
	}

	return arc
}

// getExports validates and extracts exported objects
func (b *Bundle) getExports(logger logrus.FieldLogger, rt *goja.Runtime, options bool) error {
	names := b.Module.GetExportedNames()

	for _, name := range names {
		b.exports[name] = goja.Callable(nil)
	}
	exportsV := rt.Get("exports")
	if goja.IsNull(exportsV) || goja.IsUndefined(exportsV) {
		return errors.New("exports must be an object")
	}
	exports := exportsV.ToObject(rt)

	for _, k := range exports.Keys() {
		v := exports.Get(k)
		if fn, ok := goja.AssertFunction(v); ok && k != consts.Options {
			b.exports[k] = fn
			continue
		}
		switch k {
		case consts.Options:
			if !options {
				continue
			}
			data, err := json.Marshal(v.Export())
			if err != nil {
				return err
			}
			dec := json.NewDecoder(bytes.NewReader(data))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&b.Options); err != nil {
				if uerr := json.Unmarshal(data, &b.Options); uerr != nil {
					return uerr
				}
				logger.WithError(err).Warn("There were unknown fields in the options exported in the script")
			}
		case consts.SetupFn:
			return errors.New("exported 'setup' must be a function")
		case consts.TeardownFn:
			return errors.New("exported 'teardown' must be a function")
		}
	}

	if len(b.exports) == 0 {
		return errors.New("no exported functions in script")
	}

	return nil
}

// Instantiate creates a new runtime from this bundle.
func (b *Bundle) Instantiate(
	logger logrus.FieldLogger, vuID uint64, vuImpl *moduleVUImpl,
) (bi *BundleInstance, instErr error) {
	// Instantiate the bundle into a new VM using a bound init context. This uses a context with a
	// runtime, but no state, to allow module-provided types to function within the init context.
	vuImpl.runtime = goja.New()
	init := newBoundInitContext(b.BaseInitContext, vuImpl)
	var mi goja.ModuleInstance
	var err error
	if mi, err = b.instantiate(logger, vuImpl.runtime, init, vuID); err != nil {
		return nil, err
	}

	rt := vuImpl.runtime
	bi = &BundleInstance{
		Runtime:        rt,
		Context:        &vuImpl.ctx,
		exports:        make(map[string]goja.Callable),
		env:            b.RuntimeOptions.Env,
		ModuleInstance: mi,
	}

	for _, name := range b.Module.GetExportedNames() {
		v := mi.GetBindingValue(unistring.String(name), false)
		fn, ok := goja.AssertFunction(v)
		if ok {
			bi.exports[name] = fn
		}
	}
	/*
		// Grab any exported functions that could be executed. These were
		// already pre-validated in cmd.validateScenarioConfig(), just get them here.
		exports := rt.Get("exports").ToObject(rt)
		for k := range b.exports {
			fn, _ := goja.AssertFunction(exports.Get(k))
			bi.exports[k] = fn
		}

		jsOptions := rt.Get("options")
		var jsOptionsObj *goja.Object
		if jsOptions == nil || goja.IsNull(jsOptions) || goja.IsUndefined(jsOptions) {
			jsOptionsObj = rt.NewObject()
			rt.Set("options", jsOptionsObj)
		} else {
			jsOptionsObj = jsOptions.ToObject(rt)
		}
		b.Options.ForEachSpecified("json", func(key string, val interface{}) {
			if err := jsOptionsObj.Set(key, val); err != nil {
				instErr = err
			}
		})
	*/

	return bi, instErr
}

// fix
type vugetter struct {
	vu *moduleVUImpl
}

func (v vugetter) get() *moduleVUImpl {
	return v.vu
}

// Instantiates the bundle into an existing runtime. Not public because it also messes with a bunch
// of other things, will potentially thrash data and makes a mess in it if the operation fails.
func (b *Bundle) instantiate(
	logger logrus.FieldLogger, rt *goja.Runtime, init *InitContext, vuID uint64,
) (goja.ModuleInstance, error) {
	rt.SetFieldNameMapper(common.FieldNameMapper{})
	rt.SetRandSource(common.NewRandSource())

	exports := rt.NewObject()
	rt.Set("exports", exports)
	module := rt.NewObject()
	_ = module.Set("exports", exports)
	rt.Set("module", module)

	env := make(map[string]string, len(b.RuntimeOptions.Env))
	for key, value := range b.RuntimeOptions.Env {
		env[key] = value
	}
	rt.Set("__ENV", env)
	rt.Set("__VU", vuID)
	_ = rt.Set("console", newConsole(logger))

	if init.compatibilityMode == lib.CompatibilityModeExtended {
		rt.Set("global", rt.GlobalObject())
	}
	rt.GlobalObject().DefineDataProperty("vugetter",
		rt.ToValue(vugetter{init.moduleVUImpl}), goja.FLAG_FALSE, goja.FLAG_FALSE, goja.FLAG_FALSE)

	// TODO: get rid of the unused ctxPtr, use a real external context (so we
	// can interrupt), build the common.InitEnvironment earlier and reuse it
	initenv := &common.InitEnvironment{
		Logger:      logger,
		FileSystems: init.filesystems,
		CWD:         init.pwd,
		Registry:    b.registry,
	}
	init.moduleVUImpl.initEnv = initenv
	init.moduleVUImpl.ctx = context.Background()
	unbindInit := b.setInitGlobals(rt, init)
	init.moduleVUImpl.eventLoop = eventloop.New(init.moduleVUImpl)
	var mi goja.ModuleInstance
	var err error
	err = common.RunWithPanicCatching(logger, rt, func() error {
		return init.moduleVUImpl.eventLoop.Start(func() error {
			/*
				_, errRun := rt.RunProgram(b.Program)
			*/
			var errRun error
			mi, errRun = b.Module.Evaluate(rt)
			return errRun
		})
	})

	if err != nil {
		var exception *goja.Exception
		if errors.As(err, &exception) {
			err = &scriptException{inner: exception}
		}
		return nil, err
	}
	unbindInit()
	init.moduleVUImpl.ctx = nil
	init.moduleVUImpl.initEnv = nil

	// If we've already initialized the original VU init context, forbid
	// any subsequent VUs to open new files
	if vuID == 0 {
		init.allowOnlyOpenedFiles()
	}

	rt.SetRandSource(common.NewRandSource())

	return mi, nil
}

func (b *Bundle) setInitGlobals(rt *goja.Runtime, init *InitContext) (unset func()) {
	mustSet := func(k string, v interface{}) {
		if err := rt.Set(k, v); err != nil {
			panic(fmt.Errorf("failed to set '%s' global object: %w", k, err))
		}
	}
	mustSet("require", init.Require)
	mustSet("open", init.Open)
	return func() {
		mustSet("require", goja.Undefined())
		mustSet("open", goja.Undefined())
	}
}

func generateSourceMapLoader(logger logrus.FieldLogger, filesystems map[string]afero.Fs,
) func(path string) ([]byte, error) {
	return func(path string) ([]byte, error) {
		u, err := url.Parse(path)
		if err != nil {
			return nil, err
		}
		data, err := loader.Load(logger, filesystems, u, path)
		if err != nil {
			return nil, err
		}
		return data.Data, nil
	}
}
