# migrationgen

`migrationgen` is a reusable Go package for generating versioned SQL migrations from GORM models.

## Install

```bash
go get github.com/Amasterr/go-migration@latest
```

## Usage

```go
package main

import (
	migrationgen "github.com/Amasterr/go-migration"
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
git push git@github.com:Amasterr/go-migration.git migrationgen-release:main
```

Then tag and push a release:

```bash
git clone git@github.com:Amasterr/go-migration.git
cd go-migration
git tag v0.1.2
git push origin v0.1.2
```
