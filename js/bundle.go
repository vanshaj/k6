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
	"sync"

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

	exports  map[string]goja.Callable
	cache    map[string]moduleCacheElement
	reverse  map[goja.ModuleRecord]*url.URL
	compiler *compiler.Compiler
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

// TODO use the first argument
func (b *Bundle) resolveModule(ref interface{}, specifier string) (goja.ModuleRecord, error) {
	if specifier == "k6" || strings.HasPrefix(specifier, "k6/") {
		mod, ok := b.BaseInitContext.modules[specifier]
		if !ok {
			return nil, fmt.Errorf("unknown module: %s", specifier)
		}
		k6m, ok := mod.(modules.Module)
		if !ok {
			return nil, fmt.Errorf("unsupported not native module " + specifier)
		}
		return wrapModule(k6m), nil
	}
	// todo fix
	var pwd *url.URL
	var main bool
	if ref == nil {
		pwd = loader.Dir(b.Filename)
		main = true
	} else if mr, ok := ref.(goja.ModuleRecord); ok {
		pwd = loader.Dir(b.reverse[mr])
	} // TODO fix this for all other cases using ref and cache
	fileurl, err := loader.Resolve(pwd, specifier)
	if err != nil {
		return nil, err
	}
	originalspecifier := specifier //nolint:ifshort
	specifier = fileurl.String()
	k, ok := b.cache[specifier]
	if ok {
		return k.m, k.err
	}
	var data string
	if originalspecifier == b.Filename.String() {
		// this mostly exists for tests ... kind of
		data = b.Source
	} else {
		var resolvedsrc *loader.SourceData
		resolvedsrc, err = loader.Load(b.BaseInitContext.logger, b.BaseInitContext.filesystems, fileurl, originalspecifier)
		if err != nil {
			return nil, err
		}
		data = string(resolvedsrc.Data)
	}

	ast, ismodule, err := b.compiler.Parse(data, specifier, true)
	if err != nil {
		b.cache[specifier] = moduleCacheElement{err: err}
		return nil, err
	}
	if !ismodule {
		b.BaseInitContext.logger.WithField("specifier", specifier).Error(
			"A not module will be evaluated. This might not work great, please don't use commonjs")
		prg, _, err := b.compiler.Compile(data, specifier, main)
		if err != nil { // TODO try something on top?
			return nil, err
		}
		return wrapCommonJS(prg, main), nil
		// todo warning
		// todo implement wrapper
	}
	m, err := goja.ModuleFromAST(specifier, ast, b.resolveModule)
	if err != nil {
		b.cache[specifier] = moduleCacheElement{err: err}
		return nil, err
	}
	b.cache[specifier] = moduleCacheElement{m: m}
	b.reverse[m] = fileurl
	return m, nil
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

	rt := goja.New()
	i := NewInitContext(logger, rt, c, compatMode, new(context.Context),
		filesystems, loader.Dir(src.URL))
	// Make a bundle, instantiate it into a throwaway VM to populate caches.
	bundle := Bundle{
		Filename: src.URL,
		Source:   code,
		// Program:  pgm,
		BaseInitContext:   i,
		RuntimeOptions:    rtOpts,
		CompatibilityMode: compatMode,
		exports:           make(map[string]goja.Callable),
		registry:          registry,
		cache:             make(map[string]moduleCacheElement),
		reverse:           make(map[goja.ModuleRecord]*url.URL),
		compiler:          c,
	}

	m, err := bundle.resolveModule(nil, src.URL.String())
	if err != nil {
		return nil, err
	}
	err = m.Link()
	if err != nil {
		return nil, err
	}
	// Make a bundle, instantiate it into a throwaway VM to populate caches.
	bundle.Module = m

	var mi goja.ModuleInstance
	if mi, err = bundle.instantiate(logger, rt, bundle.BaseInitContext, 0); err != nil {
		return nil, err
	}

	err = bundle.getExports(logger, mi, true)
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
	rt := goja.New()
	i := NewInitContext(logger, rt, c, compatMode,
		new(context.Context), arc.Filesystems, arc.PwdURL)

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
		// Program:           pgm,
		Options:           arc.Options,
		BaseInitContext:   i,
		RuntimeOptions:    rtOpts,
		CompatibilityMode: compatMode,
		exports:           make(map[string]goja.Callable),
		registry:          registry,
		cache:             make(map[string]moduleCacheElement),
		reverse:           make(map[goja.ModuleRecord]*url.URL),
		compiler:          c,
	}
	m, err := bundle.resolveModule(nil, arc.Filename)
	if err != nil {
		return nil, err
	}
	err = m.Link()
	if err != nil {
		return nil, err
	}

	bundle.Module = m
	var mi goja.ModuleInstance
	if mi, err = bundle.instantiate(logger, rt, bundle.BaseInitContext, 0); err != nil {
		return nil, err
	}

	// Grab exported objects, but avoid overwriting options, which would
	// be initialized from the metadata.json at this point.
	err = bundle.getExports(logger, mi, false)
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
func (b *Bundle) getExports(logger logrus.FieldLogger, mi goja.ModuleInstance, options bool) error {
	for _, k := range b.Module.GetExportedNames() {
		v := mi.GetBindingValue(unistring.String(k), true)
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

type wrappedCommonJS struct {
	prg           *goja.Program
	main          bool
	exportedNames []string
	o             sync.Once
}

var _ goja.ModuleRecord = &wrappedCommonJS{}

func wrapCommonJS(prg *goja.Program, main bool) goja.ModuleRecord {
	// TODO this needs to be the default IMO
	return &wrappedCommonJS{
		prg:  prg,
		main: main,
	}
}

func (w *wrappedCommonJS) Link() error {
	return nil // TDOF fix
}

func (w *wrappedCommonJS) Evaluate(rt *goja.Runtime) (goja.ModuleInstance, error) {
	// vu := rt.GlobalObject().Get("vugetter").Export().(vugetter).get() //nolint:forcetypeassert
	if _, err := rt.RunProgram(w.prg); err != nil {
		return nil, err
	}
	if !w.main {
		return nil, fmt.Errorf("unsupported require in modules for now")
	}

	exports := rt.Get("exports").ToObject(rt)
	w.o.Do(func() {
		w.exportedNames = exports.Keys()
	})
	return &wrappedCommonJSInstance{rt: rt, exports: exports, w: w}, nil // TODO fix
}

func (w wrappedCommonJS) GetExportedNames(set ...*goja.SourceTextModuleRecord) []string {
	w.o.Do(func() {
		panic("this shouldn't happen ;)")
	})
	return w.exportedNames
}

func (w *wrappedCommonJS) ResolveExport(
	exportName string, set ...goja.ResolveSetElement,
) (*goja.ResolvedBinding, bool) {
	return &goja.ResolvedBinding{
		Module:      w,
		BindingName: exportName,
	}, false
}

type wrappedCommonJSInstance struct {
	exports *goja.Object
	rt      *goja.Runtime
	w       *wrappedCommonJS
}

func (wmi *wrappedCommonJSInstance) GetBindingValue(name unistring.String, _ bool) goja.Value {
	n := name.String()
	if n == "default" {
		if wmi.w.main { // hack for just hte main file as it worked like that before :facepalm:
			d := wmi.exports.Get("default")
			if d != nil {
				return d
			}
		}
		return wmi.exports
	}
	return wmi.exports.Get(n)
}
