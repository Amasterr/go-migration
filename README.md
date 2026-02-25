# migrationgen

`migrationgen` is a reusable Go package for generating versioned SQL migrations from GORM models.

## Install

```bash
go get github.com/tuyoo/migrationgen@latest
```

## Usage

```go
package main

import (
	"github.com/tuyoo/migrationgen"
)

func main() {
	models := []any{
		&User{},
		&Order{},
	}

	// Sync snapshot only (no SQL files generated)
	statePath, err := migrationgen.SyncSchemaState(models, "./database/migrations/main", "")
	if err != nil {
		panic(err)
	}
	_ = statePath

	// Generate migration SQL from snapshot diff
	result, err := migrationgen.MakeMigrations(models, "./database/migrations/main", "add_order_index", "")
	if err != nil {
		panic(err)
	}
	if !result.Changed {
		println("No changes detected.")
		return
	}
	println(result.UpPath)
	println(result.DownPath)
}
```

## Release from This Monorepo

If this package is developed inside a monorepo, you can split and push it to its own GitHub repository:

```bash
# from monorepo root
git subtree split --prefix=pkg/migrationgen -b migrationgen-release
git push git@github.com:tuyoo/migrationgen.git migrationgen-release:main
```

Then tag and push a release:

```bash
git clone git@github.com:tuyoo/migrationgen.git
cd migrationgen
git tag v0.1.0
git push origin v0.1.0
```
