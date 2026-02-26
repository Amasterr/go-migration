package gomigration

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type relationUser struct {
	ID     uint             `gorm:"primaryKey"`
	Groups []*relationGroup `gorm:"many2many:test_user_groups;"`
}

func (relationUser) TableName() string { return "test_users" }

type relationGroup struct {
	ID    uint            `gorm:"primaryKey"`
	Users []*relationUser `gorm:"many2many:test_user_groups;"`
}

func (relationGroup) TableName() string { return "test_groups" }

type dedupeUser struct {
	ID uint `gorm:"primaryKey"`
}

func (dedupeUser) TableName() string { return "dedupe_users" }

type dedupeGroup struct {
	ID     uint          `gorm:"primaryKey"`
	UsersA []*dedupeUser `gorm:"many2many:dedupe_user_groups;"`
	UsersB []*dedupeUser `gorm:"many2many:dedupe_user_groups;"`
}

func (dedupeGroup) TableName() string { return "dedupe_groups" }

type indexOptionModel struct {
	ID   uint   `gorm:"primaryKey"`
	Name string `gorm:"index:idx_index_option_model_name,sort:desc,length:16,collate:utf8mb4_bin,type:btree,comment:index_comment"`
}

func (indexOptionModel) TableName() string { return "index_option_models" }

type partialIndexModel struct {
	ID    uint   `gorm:"primaryKey"`
	Value string `gorm:"index:idx_partial_value,where:value IS NOT NULL"`
}

func (partialIndexModel) TableName() string { return "partial_index_models" }

type compositeIndexInvalidModel struct {
	ID    uint   `gorm:"primaryKey"`
	Value string `gorm:"index:,composite"`
}

func (compositeIndexInvalidModel) TableName() string { return "composite_index_invalid_models" }

type e2eUserWithJoin struct {
	ID     uint                `gorm:"primaryKey"`
	Groups []*e2eGroupWithJoin `gorm:"many2many:e2e_user_groups;"`
}

func (e2eUserWithJoin) TableName() string { return "e2e_users" }

type e2eGroupWithJoin struct {
	ID    uint               `gorm:"primaryKey"`
	Users []*e2eUserWithJoin `gorm:"many2many:e2e_user_groups;"`
}

func (e2eGroupWithJoin) TableName() string { return "e2e_groups" }

type e2eUserNoJoin struct {
	ID uint `gorm:"primaryKey"`
}

func (e2eUserNoJoin) TableName() string { return "e2e_users" }

type e2eGroupNoJoin struct {
	ID uint `gorm:"primaryKey"`
}

func (e2eGroupNoJoin) TableName() string { return "e2e_groups" }

func migrationModels() []any {
	return []any{
		&relationUser{},
		&relationGroup{},
	}
}

func TestBuildCurrentStateIncludesMany2ManyJoinTableAndForeignKeys(t *testing.T) {
	state, err := buildCurrentState([]any{&relationUser{}, &relationGroup{}})
	if err != nil {
		t.Fatalf("buildCurrentState failed: %v", err)
	}

	join, ok := state.Tables["test_user_groups"]
	if !ok {
		t.Fatalf("join table test_user_groups not found, tables=%v", sortedKeys(state.Tables))
	}
	if len(join.Columns) != 2 {
		t.Fatalf("expected 2 join columns, got %d", len(join.Columns))
	}
	if len(join.PrimaryKeys) != 2 {
		t.Fatalf("expected 2 join primary keys, got %d", len(join.PrimaryKeys))
	}
	if len(join.ForeignKeys) != 2 {
		t.Fatalf("expected 2 join foreign keys, got %d", len(join.ForeignKeys))
	}

	refTables := map[string]bool{}
	for name, fk := range join.ForeignKeys {
		if len(fk.Columns) == 0 || len(fk.RefColumns) == 0 {
			t.Fatalf("foreign key %s has empty columns: %#v", name, fk)
		}
		refTables[fk.RefTable] = true
	}
	if !refTables["test_users"] || !refTables["test_groups"] {
		t.Fatalf("expected refs to test_users and test_groups, got %#v", join.ForeignKeys)
	}
}

func TestBuildCurrentStateDeduplicatesEquivalentForeignKeys(t *testing.T) {
	state, err := buildCurrentState([]any{&dedupeUser{}, &dedupeGroup{}})
	if err != nil {
		t.Fatalf("buildCurrentState failed: %v", err)
	}
	join, ok := state.Tables["dedupe_user_groups"]
	if !ok {
		t.Fatalf("join table dedupe_user_groups not found, tables=%v", sortedKeys(state.Tables))
	}
	if len(join.ForeignKeys) != 2 {
		t.Fatalf("expected deduplicated foreign keys to be 2, got %d (%#v)", len(join.ForeignKeys), join.ForeignKeys)
	}
}

func TestBuildDiffForNewTableIncludesForeignKeyStatements(t *testing.T) {
	parent := tableState{
		Columns: map[string]columnState{
			"id": {Definition: "bigint unsigned"},
		},
		PrimaryKeys: []string{"id"},
	}
	child := tableState{
		Columns: map[string]columnState{
			"id":        {Definition: "bigint unsigned"},
			"parent_id": {Definition: "bigint unsigned"},
		},
		PrimaryKeys: []string{"id"},
		ForeignKeys: map[string]foreignKeyState{
			"fk_children_parent": {
				Columns:    []string{"parent_id"},
				RefTable:   "parents",
				RefColumns: []string{"id"},
				OnDelete:   "CASCADE",
				OnUpdate:   "CASCADE",
			},
		},
	}

	previous := schemaState{Tables: map[string]tableState{
		"parents": parent,
	}}
	current := schemaState{Tables: map[string]tableState{
		"parents":  parent,
		"children": child,
	}}

	up, down := buildDiff(previous, current)
	if len(up) != 2 {
		t.Fatalf("expected 2 up statements (create table + add fk), got %d: %v", len(up), up)
	}
	if !strings.Contains(up[0], "CREATE TABLE `children`") {
		t.Fatalf("expected first up SQL to create children table, got: %s", up[0])
	}
	if !strings.Contains(up[1], "ADD CONSTRAINT `fk_children_parent`") {
		t.Fatalf("expected second up SQL to add fk, got: %s", up[1])
	}
	if !strings.Contains(up[1], "ON DELETE CASCADE") || !strings.Contains(up[1], "ON UPDATE CASCADE") {
		t.Fatalf("expected fk actions in up SQL, got: %s", up[1])
	}

	if len(down) != 2 {
		t.Fatalf("expected 2 down statements (drop fk + drop table), got %d: %v", len(down), down)
	}
	if !strings.Contains(down[0], "DROP FOREIGN KEY `fk_children_parent`") {
		t.Fatalf("expected first down SQL to drop fk, got: %s", down[0])
	}
	if !strings.Contains(down[1], "DROP TABLE IF EXISTS `children`") {
		t.Fatalf("expected second down SQL to drop table, got: %s", down[1])
	}
}

func TestDiffForeignKeysModifyConstraint(t *testing.T) {
	prev := map[string]foreignKeyState{
		"fk_children_parent": {
			Columns:    []string{"parent_id"},
			RefTable:   "parents",
			RefColumns: []string{"id"},
			OnDelete:   "CASCADE",
			OnUpdate:   "CASCADE",
		},
	}
	cur := map[string]foreignKeyState{
		"fk_children_parent": {
			Columns:    []string{"parent_id"},
			RefTable:   "parents",
			RefColumns: []string{"id"},
			OnDelete:   "SET NULL",
			OnUpdate:   "CASCADE",
		},
	}

	dropOps, addOps := diffForeignKeys("children", prev, cur)
	if len(dropOps) != 1 || len(addOps) != 1 {
		t.Fatalf("expected one drop op and one add op, got drop=%d add=%d", len(dropOps), len(addOps))
	}
	if !strings.Contains(dropOps[0].up, "DROP FOREIGN KEY `fk_children_parent`") {
		t.Fatalf("unexpected drop op SQL: %s", dropOps[0].up)
	}
	if !strings.Contains(addOps[0].up, "ON DELETE SET NULL") {
		t.Fatalf("expected updated on delete action in add op SQL, got: %s", addOps[0].up)
	}
}

func TestBuildCurrentStateParsesIndexTagOptions(t *testing.T) {
	state, err := buildCurrentState([]any{&indexOptionModel{}})
	if err != nil {
		t.Fatalf("buildCurrentState failed: %v", err)
	}
	table, ok := state.Tables["index_option_models"]
	if !ok {
		t.Fatalf("table index_option_models not found")
	}
	idx, ok := table.Indexes["idx_index_option_model_name"]
	if !ok {
		t.Fatalf("expected index idx_index_option_model_name, got %#v", table.Indexes)
	}
	if !strings.EqualFold(idx.Type, "btree") {
		t.Fatalf("expected index type btree, got %q", idx.Type)
	}
	if idx.Comment != "index_comment" {
		t.Fatalf("expected index comment index_comment, got %q", idx.Comment)
	}
	if len(idx.Fields) != 1 {
		t.Fatalf("expected one index field, got %d", len(idx.Fields))
	}
	field := idx.Fields[0]
	if field.Length != 16 {
		t.Fatalf("expected index prefix length 16, got %d", field.Length)
	}
	if field.Sort != "DESC" {
		t.Fatalf("expected index sort DESC, got %q", field.Sort)
	}
	if field.Collate != "utf8mb4_bin" {
		t.Fatalf("expected index collate utf8mb4_bin, got %q", field.Collate)
	}
}

func TestBuildCurrentStateRejectsPartialIndexTag(t *testing.T) {
	_, err := buildCurrentState([]any{&partialIndexModel{}})
	if err == nil {
		t.Fatalf("expected error for partial index where tag, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported for MySQL migrations") {
		t.Fatalf("expected unsupported mysql index error, got: %v", err)
	}
}

func TestStateLoadSaveRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	loaded, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState on missing file failed: %v", err)
	}
	if len(loaded.Tables) != 0 {
		t.Fatalf("expected empty tables on missing file, got %#v", loaded.Tables)
	}

	want := schemaState{
		Tables: map[string]tableState{
			"t_users": {
				Columns: map[string]columnState{
					"id":   {Definition: "bigint unsigned"},
					"name": {Definition: "varchar(128)"},
				},
				Indexes: map[string]indexState{
					"idx_t_users_name": {
						Class: "UNIQUE",
						Fields: []indexFieldState{
							{Column: "name", Sort: "DESC"},
						},
					},
				},
				ForeignKeys: map[string]foreignKeyState{
					"fk_t_users_org": {
						Columns:    []string{"org_id"},
						RefTable:   "orgs",
						RefColumns: []string{"id"},
						OnDelete:   "SET NULL",
						OnUpdate:   "CASCADE",
					},
				},
				PrimaryKeys: []string{"id"},
			},
		},
	}
	if err := saveState(path, want); err != nil {
		t.Fatalf("saveState failed: %v", err)
	}
	got, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState after save failed: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded state mismatch.\nwant=%#v\ngot=%#v", want, got)
	}
}

func TestIndexSQLHelpers(t *testing.T) {
	idx := indexState{
		Class:   "unique",
		Type:    "btree",
		Comment: "O'Brien",
		Option:  "WITH PARSER ngram",
		Fields: []indexFieldState{
			{
				Column:  "name",
				Length:  16,
				Collate: "utf8mb4_bin",
				Sort:    "desc",
			},
		},
	}
	gotCreate := createIndexSQL("users", "idx_users_name", idx)
	wantCreate := "CREATE UNIQUE INDEX `idx_users_name` ON `users` (`name`(16) COLLATE utf8mb4_bin DESC) USING btree COMMENT 'O''Brien' WITH PARSER ngram;"
	if gotCreate != wantCreate {
		t.Fatalf("unexpected create index SQL.\nwant=%s\ngot=%s", wantCreate, gotCreate)
	}

	gotDef := createTableIndexDefinition("idx_users_name", idx)
	wantDef := "UNIQUE KEY `idx_users_name` (`name`(16) COLLATE utf8mb4_bin DESC) COMMENT 'O''Brien' WITH PARSER ngram"
	if gotDef != wantDef {
		t.Fatalf("unexpected table index definition.\nwant=%s\ngot=%s", wantDef, gotDef)
	}

	gotDrop := dropIndexSQL("users", "idx_users_name")
	wantDrop := "DROP INDEX `idx_users_name` ON `users`;"
	if gotDrop != wantDrop {
		t.Fatalf("unexpected drop index SQL.\nwant=%s\ngot=%s", wantDrop, gotDrop)
	}
}

func TestDiffTableWithColumnAndIndexChanges(t *testing.T) {
	prev := tableState{
		Columns: map[string]columnState{
			"a": {Definition: "bigint unsigned"},
			"b": {Definition: "varchar(16)"},
		},
		Indexes: map[string]indexState{
			"idx_a": {Fields: []indexFieldState{{Column: "a"}}},
		},
	}
	cur := tableState{
		Columns: map[string]columnState{
			"b": {Definition: "varchar(32)"},
			"c": {Definition: "bigint unsigned"},
		},
		Indexes: map[string]indexState{
			"idx_b": {Class: "UNIQUE", Fields: []indexFieldState{{Column: "b"}}},
		},
	}

	ops := diffTable("demo", prev, cur)
	upSQL := make([]string, 0, len(ops))
	for _, op := range ops {
		upSQL = append(upSQL, op.up)
	}
	joined := strings.Join(upSQL, "\n")

	mustContain := []string{
		"ALTER TABLE `demo` ADD COLUMN `c` bigint unsigned;",
		"ALTER TABLE `demo` MODIFY COLUMN `b` varchar(32);",
		"ALTER TABLE `demo` DROP COLUMN `a`;",
		"CREATE UNIQUE INDEX `idx_b` ON `demo` (`b`);",
		"DROP INDEX `idx_a` ON `demo`;",
	}
	for _, s := range mustContain {
		if !strings.Contains(joined, s) {
			t.Fatalf("expected SQL fragment not found: %s\nall=%s", s, joined)
		}
	}
}

func TestDiffTableAddsForeignKeyWhenPreviousHadNone(t *testing.T) {
	prev := tableState{
		Columns: map[string]columnState{
			"id":         {Definition: "bigint unsigned"},
			"creator_id": {Definition: "bigint unsigned"},
		},
		PrimaryKeys: []string{"id"},
	}
	cur := tableState{
		Columns: map[string]columnState{
			"id":         {Definition: "bigint unsigned"},
			"creator_id": {Definition: "bigint unsigned"},
		},
		PrimaryKeys: []string{"id"},
		ForeignKeys: map[string]foreignKeyState{
			"fk_demo_creator": {
				Columns:    []string{"creator_id"},
				RefTable:   "creators",
				RefColumns: []string{"id"},
				OnDelete:   "SET NULL",
				OnUpdate:   "CASCADE",
			},
		},
	}

	ops := diffTable("demo", prev, cur)
	if len(ops) == 0 {
		t.Fatalf("expected at least one operation when adding first foreign key")
	}

	found := false
	for _, op := range ops {
		if strings.Contains(op.up, "ADD CONSTRAINT `fk_demo_creator`") &&
			strings.Contains(op.up, "FOREIGN KEY (`creator_id`) REFERENCES `creators` (`id`)") {
			found = true
			break
		}
	}
	if !found {
		up := make([]string, 0, len(ops))
		for _, op := range ops {
			up = append(up, op.up)
		}
		t.Fatalf("expected foreign key add SQL not found, got: %v", up)
	}
}

func TestBuildDiffCoversAllSchemaChangeTypes(t *testing.T) {
	base := func() tableState {
		return tableState{
			Columns: map[string]columnState{
				"id": {Definition: "bigint unsigned AUTO_INCREMENT"},
			},
			PrimaryKeys: []string{"id"},
		}
	}

	prev := schemaState{
		Tables: map[string]tableState{
			"profiles":        base(),
			"legacy_profiles": base(),
			"users":           base(),
			"parents":         base(),
			"alpha": {
				Columns: map[string]columnState{
					"id":                {Definition: "bigint unsigned AUTO_INCREMENT"},
					"profile_id":        {Definition: "bigint unsigned"},
					"legacy_profile_id": {Definition: "bigint unsigned"},
					"creator_id":        {Definition: "bigint unsigned"},
					"name":              {Definition: "varchar(32)"},
					"old_flag":          {Definition: "tinyint unsigned DEFAULT 0"},
				},
				Indexes: map[string]indexState{
					"idx_alpha_old_flag": {Fields: []indexFieldState{{Column: "old_flag"}}},
					"idx_alpha_mut":      {Fields: []indexFieldState{{Column: "profile_id"}}},
				},
				ForeignKeys: map[string]foreignKeyState{
					"fk_alpha_profile": {
						Columns:    []string{"profile_id"},
						RefTable:   "profiles",
						RefColumns: []string{"id"},
						OnDelete:   "SET NULL",
						OnUpdate:   "CASCADE",
					},
					"fk_alpha_legacy": {
						Columns:    []string{"legacy_profile_id"},
						RefTable:   "legacy_profiles",
						RefColumns: []string{"id"},
						OnDelete:   "CASCADE",
						OnUpdate:   "CASCADE",
					},
				},
				PrimaryKeys: []string{"id"},
			},
			"obsolete_links": {
				Columns: map[string]columnState{
					"id":        {Definition: "bigint unsigned AUTO_INCREMENT"},
					"parent_id": {Definition: "bigint unsigned"},
				},
				ForeignKeys: map[string]foreignKeyState{
					"fk_obsolete_links_parent": {
						Columns:    []string{"parent_id"},
						RefTable:   "parents",
						RefColumns: []string{"id"},
						OnDelete:   "CASCADE",
					},
				},
				PrimaryKeys: []string{"id"},
			},
		},
	}

	cur := schemaState{
		Tables: map[string]tableState{
			"profiles":        base(),
			"legacy_profiles": base(),
			"users":           base(),
			"parents":         base(),
			"alpha": {
				Columns: map[string]columnState{
					"id":         {Definition: "bigint unsigned AUTO_INCREMENT"},
					"profile_id": {Definition: "bigint unsigned"},
					"creator_id": {Definition: "bigint unsigned"},
					"name":       {Definition: "varchar(128)"},
					"status":     {Definition: "tinyint unsigned DEFAULT 1"},
				},
				Indexes: map[string]indexState{
					"idx_alpha_status": {Fields: []indexFieldState{{Column: "status"}}},
					"idx_alpha_mut":    {Class: "UNIQUE", Fields: []indexFieldState{{Column: "profile_id"}}},
				},
				ForeignKeys: map[string]foreignKeyState{
					"fk_alpha_profile": {
						Columns:    []string{"profile_id"},
						RefTable:   "profiles",
						RefColumns: []string{"id"},
						OnDelete:   "CASCADE",
						OnUpdate:   "CASCADE",
					},
					"fk_alpha_creator": {
						Columns:    []string{"creator_id"},
						RefTable:   "users",
						RefColumns: []string{"id"},
						OnDelete:   "SET NULL",
						OnUpdate:   "CASCADE",
					},
				},
				PrimaryKeys: []string{"id"},
			},
			"beta_links": {
				Columns: map[string]columnState{
					"id":       {Definition: "bigint unsigned AUTO_INCREMENT"},
					"alpha_id": {Definition: "bigint unsigned"},
				},
				Indexes: map[string]indexState{
					"idx_beta_links_alpha": {Fields: []indexFieldState{{Column: "alpha_id"}}},
				},
				ForeignKeys: map[string]foreignKeyState{
					"fk_beta_links_alpha": {
						Columns:    []string{"alpha_id"},
						RefTable:   "alpha",
						RefColumns: []string{"id"},
						OnDelete:   "CASCADE",
						OnUpdate:   "CASCADE",
					},
				},
				PrimaryKeys: []string{"id"},
			},
		},
	}

	up, down := buildDiff(prev, cur)
	upJoined := strings.Join(up, "\n")
	downJoined := strings.Join(down, "\n")

	assertContainsAll(t, upJoined, []string{
		"CREATE TABLE `beta_links`",
		"DROP TABLE IF EXISTS `obsolete_links`;",
		"ALTER TABLE `alpha` ADD COLUMN `status` tinyint unsigned DEFAULT 1;",
		"ALTER TABLE `alpha` DROP COLUMN `legacy_profile_id`;",
		"ALTER TABLE `alpha` MODIFY COLUMN `name` varchar(128);",
		"CREATE INDEX `idx_alpha_status` ON `alpha` (`status`);",
		"DROP INDEX `idx_alpha_old_flag` ON `alpha`;",
		"DROP INDEX `idx_alpha_mut` ON `alpha`;",
		"CREATE UNIQUE INDEX `idx_alpha_mut` ON `alpha` (`profile_id`);",
		"ALTER TABLE `alpha` DROP FOREIGN KEY `fk_alpha_legacy`;",
		"ALTER TABLE `alpha` DROP FOREIGN KEY `fk_alpha_profile`;",
		"ALTER TABLE `alpha` ADD CONSTRAINT `fk_alpha_profile` FOREIGN KEY (`profile_id`) REFERENCES `profiles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;",
		"ALTER TABLE `alpha` ADD CONSTRAINT `fk_alpha_creator` FOREIGN KEY (`creator_id`) REFERENCES `users` (`id`) ON DELETE SET NULL ON UPDATE CASCADE;",
		"ALTER TABLE `beta_links` ADD CONSTRAINT `fk_beta_links_alpha` FOREIGN KEY (`alpha_id`) REFERENCES `alpha` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;",
	})

	assertContainsAll(t, downJoined, []string{
		"ALTER TABLE `beta_links` DROP FOREIGN KEY `fk_beta_links_alpha`;",
		"DROP TABLE IF EXISTS `beta_links`;",
		"CREATE TABLE `obsolete_links`",
		"ALTER TABLE `obsolete_links` ADD CONSTRAINT `fk_obsolete_links_parent` FOREIGN KEY (`parent_id`) REFERENCES `parents` (`id`) ON DELETE CASCADE;",
		"ALTER TABLE `alpha` DROP FOREIGN KEY `fk_alpha_creator`;",
		"ALTER TABLE `alpha` DROP FOREIGN KEY `fk_alpha_profile`;",
		"ALTER TABLE `alpha` ADD CONSTRAINT `fk_alpha_profile` FOREIGN KEY (`profile_id`) REFERENCES `profiles` (`id`) ON DELETE SET NULL ON UPDATE CASCADE;",
		"ALTER TABLE `alpha` ADD CONSTRAINT `fk_alpha_legacy` FOREIGN KEY (`legacy_profile_id`) REFERENCES `legacy_profiles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;",
		"DROP INDEX `idx_alpha_status` ON `alpha`;",
		"CREATE INDEX `idx_alpha_old_flag` ON `alpha` (`old_flag`);",
		"DROP INDEX `idx_alpha_mut` ON `alpha`;",
		"CREATE INDEX `idx_alpha_mut` ON `alpha` (`profile_id`);",
		"ALTER TABLE `alpha` DROP COLUMN `status`;",
		"ALTER TABLE `alpha` ADD COLUMN `legacy_profile_id` bigint unsigned;",
		"ALTER TABLE `alpha` MODIFY COLUMN `name` varchar(32);",
	})
}

func TestBuildDiffMany2ManyRelationCreateAndDrop(t *testing.T) {
	withoutJoin, err := buildCurrentState([]any{&e2eUserNoJoin{}, &e2eGroupNoJoin{}})
	if err != nil {
		t.Fatalf("buildCurrentState without join failed: %v", err)
	}
	withJoin, err := buildCurrentState([]any{&e2eUserWithJoin{}, &e2eGroupWithJoin{}})
	if err != nil {
		t.Fatalf("buildCurrentState with join failed: %v", err)
	}

	upCreate, downCreate := buildDiff(withoutJoin, withJoin)
	upCreateJoined := strings.Join(upCreate, "\n")
	downCreateJoined := strings.Join(downCreate, "\n")

	assertContainsAll(t, upCreateJoined, []string{
		"CREATE TABLE `e2e_user_groups`",
		"REFERENCES `e2e_users` (`id`)",
		"REFERENCES `e2e_groups` (`id`)",
	})
	assertContainsAll(t, downCreateJoined, []string{
		"DROP FOREIGN KEY",
		"DROP TABLE IF EXISTS `e2e_user_groups`;",
	})

	upDrop, downDrop := buildDiff(withJoin, withoutJoin)
	upDropJoined := strings.Join(upDrop, "\n")
	downDropJoined := strings.Join(downDrop, "\n")

	assertContainsAll(t, upDropJoined, []string{
		"DROP TABLE IF EXISTS `e2e_user_groups`;",
	})
	assertContainsAll(t, downDropJoined, []string{
		"CREATE TABLE `e2e_user_groups`",
		"REFERENCES `e2e_users` (`id`)",
		"REFERENCES `e2e_groups` (`id`)",
	})
}

func TestBuildDiffDropTableRestoresForeignKeysOnDown(t *testing.T) {
	prev := schemaState{
		Tables: map[string]tableState{
			"child": {
				Columns: map[string]columnState{
					"id":        {Definition: "bigint unsigned"},
					"parent_id": {Definition: "bigint unsigned"},
				},
				PrimaryKeys: []string{"id"},
				ForeignKeys: map[string]foreignKeyState{
					"fk_child_parent": {
						Columns:    []string{"parent_id"},
						RefTable:   "parent",
						RefColumns: []string{"id"},
						OnDelete:   "CASCADE",
					},
				},
			},
		},
	}
	cur := schemaState{Tables: map[string]tableState{}}

	up, down := buildDiff(prev, cur)
	if len(up) != 1 {
		t.Fatalf("expected only drop-table in up SQL, got %d: %v", len(up), up)
	}
	if up[0] != "DROP TABLE IF EXISTS `child`;" {
		t.Fatalf("unexpected up SQL: %s", up[0])
	}
	if len(down) != 2 {
		t.Fatalf("expected create table + create fk in down SQL, got %d: %v", len(down), down)
	}
	if !strings.Contains(down[0], "CREATE TABLE `child`") {
		t.Fatalf("expected first down SQL create table, got: %s", down[0])
	}
	if !strings.Contains(down[1], "ADD CONSTRAINT `fk_child_parent`") {
		t.Fatalf("expected second down SQL create fk, got: %s", down[1])
	}
}

func TestValidateParsedIndexTagsReturnsErrorWhenIndexMissing(t *testing.T) {
	db, cleanup, err := newDryRunMySQL()
	if err != nil {
		t.Fatalf("newDryRunMySQL failed: %v", err)
	}
	defer cleanup()

	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(&indexOptionModel{}); err != nil {
		t.Fatalf("stmt.Parse failed: %v", err)
	}
	err = validateParsedIndexTags(stmt, map[string]schema.Index{})
	if err == nil {
		t.Fatalf("expected validateParsedIndexTags to fail when parsed index map is empty")
	}
	if !strings.Contains(err.Error(), "was not parsed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseFieldIndexTagDeclsCompositeEmptyError(t *testing.T) {
	db, cleanup, err := newDryRunMySQL()
	if err != nil {
		t.Fatalf("newDryRunMySQL failed: %v", err)
	}
	defer cleanup()

	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(&compositeIndexInvalidModel{}); err != nil {
		t.Fatalf("stmt.Parse failed: %v", err)
	}
	field := stmt.Schema.FieldsByName["Value"]
	if field == nil {
		t.Fatalf("field Value not found")
	}
	_, err = parseFieldIndexTagDecls(stmt.Schema.Table, field, schema.NamingStrategy{})
	if err == nil {
		t.Fatalf("expected parseFieldIndexTagDecls to fail for empty composite index")
	}
	if !strings.Contains(err.Error(), "invalid empty composite index tag") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMakeMigrationsArgValidation(t *testing.T) {
	if _, err := MakeMigrations(migrationModels(), t.TempDir(), "", ""); err == nil {
		t.Fatalf("expected --name required error")
	}
}

func TestSanitizeName(t *testing.T) {
	cases := map[string]string{
		"Add User Avatar":          "add_user_avatar",
		"  add---field###name  ":   "add_field_name",
		"____":                     "auto_migration",
		"中文 name with 123":         "name_with_123",
		"already_ok":               "already_ok",
		"mix.UPPER.and-lower_1234": "mix_upper_and_lower_1234",
	}
	for in, want := range cases {
		if got := sanitizeName(in); got != want {
			t.Fatalf("sanitizeName(%q) mismatch: want=%q got=%q", in, want, got)
		}
	}
}

func TestRunSyncStateCreatesSchemaSnapshot(t *testing.T) {
	dir := t.TempDir()
	path, err := SyncSchemaState(migrationModels(), dir, "")
	if err != nil {
		t.Fatalf("SyncSchemaState failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected schema state file at %s, got stat error: %v", path, err)
	}

	state, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState after sync failed: %v", err)
	}
	if len(state.Tables) == 0 {
		t.Fatalf("expected non-empty tables after syncstate")
	}
}

func TestRunMakeMigrationsNoChangeAfterSyncState(t *testing.T) {
	dir := t.TempDir()
	models := migrationModels()
	if _, err := SyncSchemaState(models, dir, ""); err != nil {
		t.Fatalf("SyncSchemaState failed: %v", err)
	}
	result, err := MakeMigrations(models, dir, "no_change", "")
	if err != nil {
		t.Fatalf("MakeMigrations failed: %v", err)
	}
	if result.Changed {
		t.Fatalf("expected no changes after sync state")
	}
	upFiles, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		t.Fatalf("glob up sql failed: %v", err)
	}
	downFiles, err := filepath.Glob(filepath.Join(dir, "*.down.sql"))
	if err != nil {
		t.Fatalf("glob down sql failed: %v", err)
	}
	if len(upFiles) != 0 || len(downFiles) != 0 {
		t.Fatalf("expected no migration SQL files when no changes; up=%v down=%v", upFiles, downFiles)
	}
}

func TestLoadStateInvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{invalid"), 0o644); err != nil {
		t.Fatalf("write invalid json failed: %v", err)
	}
	if _, err := loadState(path); err == nil {
		t.Fatalf("expected loadState to fail on invalid JSON")
	}
}

func TestRunMakeMigrationsCreatesSQLFiles(t *testing.T) {
	dir := t.TempDir()
	result, err := MakeMigrations(migrationModels(), dir, "init_schema", "")
	if err != nil {
		t.Fatalf("MakeMigrations failed: %v", err)
	}
	if !result.Changed {
		t.Fatalf("expected initial migration to generate files")
	}
	upFiles, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		t.Fatalf("glob up sql failed: %v", err)
	}
	downFiles, err := filepath.Glob(filepath.Join(dir, "*.down.sql"))
	if err != nil {
		t.Fatalf("glob down sql failed: %v", err)
	}
	if len(upFiles) != 1 || len(downFiles) != 1 {
		t.Fatalf("expected one up and one down migration file, got up=%v down=%v", upFiles, downFiles)
	}
	if _, err := os.Stat(filepath.Join(dir, ".schema_state.json")); err != nil {
		t.Fatalf("expected schema state file created, got: %v", err)
	}
}

func assertContainsAll(t *testing.T, sql string, fragments []string) {
	t.Helper()
	for _, fragment := range fragments {
		if strings.Contains(sql, fragment) {
			continue
		}
		t.Fatalf("expected SQL fragment not found: %s\nall SQL:\n%s", fragment, sql)
	}
}

func TestIndexClassPrefixAndKeyPrefix(t *testing.T) {
	if got := indexClassPrefix("unique"); got != "UNIQUE " {
		t.Fatalf("unexpected unique class prefix: %q", got)
	}
	if got := indexClassPrefix("fulltext"); got != "FULLTEXT " {
		t.Fatalf("unexpected fulltext class prefix: %q", got)
	}
	if got := indexClassPrefix("spatial"); got != "SPATIAL " {
		t.Fatalf("unexpected spatial class prefix: %q", got)
	}
	if got := indexClassPrefix("normal"); got != "" {
		t.Fatalf("unexpected default class prefix: %q", got)
	}

	if got := indexClassKeyPrefix("unique"); got != "UNIQUE KEY" {
		t.Fatalf("unexpected unique key prefix: %q", got)
	}
	if got := indexClassKeyPrefix("fulltext"); got != "FULLTEXT KEY" {
		t.Fatalf("unexpected fulltext key prefix: %q", got)
	}
	if got := indexClassKeyPrefix("spatial"); got != "SPATIAL KEY" {
		t.Fatalf("unexpected spatial key prefix: %q", got)
	}
	if got := indexClassKeyPrefix("normal"); got != "KEY" {
		t.Fatalf("unexpected default key prefix: %q", got)
	}
}

func TestIndexContainsDeclByColumnAndExpression(t *testing.T) {
	idx := schema.Index{
		Name: "idx_demo",
		Fields: []schema.IndexOption{
			{
				Field: &schema.Field{DBName: "name"},
			},
			{
				Expression: "LOWER(email)",
			},
		},
	}
	if !indexContainsDecl(idx, indexTagDecl{Column: "name"}) {
		t.Fatalf("expected indexContainsDecl by column to be true")
	}
	if !indexContainsDecl(idx, indexTagDecl{Expression: "LOWER(email)"}) {
		t.Fatalf("expected indexContainsDecl by expression to be true")
	}
	if indexContainsDecl(idx, indexTagDecl{Column: "missing"}) {
		t.Fatalf("expected indexContainsDecl for missing column to be false")
	}
}
