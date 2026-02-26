package gomigration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type schemaState struct {
	Tables map[string]tableState `json:"tables"`
}

type tableState struct {
	Columns     map[string]columnState     `json:"columns"`
	Indexes     map[string]indexState      `json:"indexes,omitempty"`
	ForeignKeys map[string]foreignKeyState `json:"foreign_keys,omitempty"`
	PrimaryKeys []string                   `json:"primary_keys,omitempty"`
}

type columnState struct {
	Definition string `json:"definition"`
}

type indexState struct {
	Class   string            `json:"class,omitempty"`
	Type    string            `json:"type,omitempty"`
	Where   string            `json:"where,omitempty"`
	Comment string            `json:"comment,omitempty"`
	Option  string            `json:"option,omitempty"`
	Fields  []indexFieldState `json:"fields"`
}

type indexFieldState struct {
	Column     string `json:"column,omitempty"`
	Expression string `json:"expression,omitempty"`
	Sort       string `json:"sort,omitempty"`
	Collate    string `json:"collate,omitempty"`
	Length     int    `json:"length,omitempty"`
}

type foreignKeyState struct {
	Columns    []string `json:"columns"`
	RefTable   string   `json:"ref_table"`
	RefColumns []string `json:"ref_columns"`
	OnDelete   string   `json:"on_delete,omitempty"`
	OnUpdate   string   `json:"on_update,omitempty"`
}

type indexTagDecl struct {
	Name       string
	Column     string
	Expression string
	Raw        string
}

type migrationOp struct {
	up   string
	down string
}

type MakeMigrationsResult struct {
	Changed   bool
	UpPath    string
	DownPath  string
	StatePath string
}

func MakeMigrations(models []any, dir, name, stateFile string) (MakeMigrationsResult, error) {
	result := MakeMigrationsResult{}
	if strings.TrimSpace(name) == "" {
		return result, fmt.Errorf("--name is required")
	}
	if strings.TrimSpace(dir) == "" {
		dir = filepath.Join("database", "migrations")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return result, err
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return result, err
	}
	if strings.TrimSpace(stateFile) == "" {
		stateFile = filepath.Join(absDir, ".schema_state.json")
	}
	absStateFile, err := filepath.Abs(stateFile)
	if err != nil {
		return result, err
	}
	result.StatePath = absStateFile

	previous, err := loadState(absStateFile)
	if err != nil {
		return result, err
	}
	current, err := buildCurrentState(models)
	if err != nil {
		return result, err
	}

	upSQL, downSQL := buildDiff(previous, current)
	if len(upSQL) == 0 {
		return result, nil
	}

	version := time.Now().Format("20060102150405")
	fileName := fmt.Sprintf("%s_%s", version, sanitizeName(name))
	upPath := filepath.Join(absDir, fileName+".up.sql")
	downPath := filepath.Join(absDir, fileName+".down.sql")

	if err := os.WriteFile(upPath, []byte(strings.Join(upSQL, "\n\n")+"\n"), 0o644); err != nil {
		return result, err
	}
	if err := os.WriteFile(downPath, []byte(strings.Join(downSQL, "\n\n")+"\n"), 0o644); err != nil {
		return result, err
	}
	if err := saveState(absStateFile, current); err != nil {
		return result, err
	}

	result.Changed = true
	result.UpPath = upPath
	result.DownPath = downPath
	return result, nil
}

func SyncSchemaState(models []any, dir, stateFile string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		dir = filepath.Join("database", "migrations")
	}

	absDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return "", err
	}
	if strings.TrimSpace(stateFile) == "" {
		stateFile = filepath.Join(absDir, ".schema_state.json")
	}
	absStateFile, err := filepath.Abs(stateFile)
	if err != nil {
		return "", err
	}
	current, err := buildCurrentState(models)
	if err != nil {
		return "", err
	}
	if err := saveState(absStateFile, current); err != nil {
		return "", err
	}
	return absStateFile, nil
}

func loadState(path string) (schemaState, error) {
	state := schemaState{Tables: map[string]tableState{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return schemaState{}, err
	}
	if state.Tables == nil {
		state.Tables = map[string]tableState{}
	}
	return state, nil
}

func saveState(path string, state schemaState) error {
	if state.Tables == nil {
		state.Tables = map[string]tableState{}
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func buildCurrentState(models []any) (schemaState, error) {
	db, cleanup, err := newDryRunMySQL()
	if err != nil {
		return schemaState{}, err
	}
	defer cleanup()

	schemas, err := collectSchemas(db, models)
	if err != nil {
		return schemaState{}, err
	}
	foreignKeysByTable, err := collectForeignKeysByTable(schemas)
	if err != nil {
		return schemaState{}, err
	}

	state := schemaState{Tables: map[string]tableState{}}
	for _, tableName := range sortedKeys(schemas) {
		table, err := buildTableState(db, schemas[tableName])
		if err != nil {
			return schemaState{}, err
		}
		if table.ForeignKeys == nil {
			table.ForeignKeys = map[string]foreignKeyState{}
		}
		if fks := foreignKeysByTable[tableName]; len(fks) > 0 {
			table.ForeignKeys = fks
		}
		state.Tables[tableName] = table
	}
	return state, nil
}

func collectSchemas(db *gorm.DB, models []any) (map[string]*schema.Schema, error) {
	schemas := map[string]*schema.Schema{}
	for _, m := range models {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(m); err != nil {
			return nil, err
		}
		if stmt.Schema == nil {
			continue
		}
		addSchemaAndJoinTables(schemas, stmt.Schema)
	}
	return schemas, nil
}

func addSchemaAndJoinTables(schemas map[string]*schema.Schema, sc *schema.Schema) {
	if sc == nil || strings.TrimSpace(sc.Table) == "" {
		return
	}
	if _, exists := schemas[sc.Table]; !exists {
		schemas[sc.Table] = sc
	}
	walkRelationships(&sc.Relationships, func(rel *schema.Relationship) {
		if rel == nil || rel.JoinTable == nil {
			return
		}
		addSchemaAndJoinTables(schemas, rel.JoinTable)
	})
}

func walkRelationships(rels *schema.Relationships, fn func(*schema.Relationship)) {
	if rels == nil || fn == nil {
		return
	}
	for _, name := range sortedKeys(rels.Relations) {
		fn(rels.Relations[name])
	}
	for _, name := range sortedKeys(rels.EmbeddedRelations) {
		walkRelationships(rels.EmbeddedRelations[name], fn)
	}
}

func buildTableState(db *gorm.DB, sc *schema.Schema) (tableState, error) {
	if sc == nil {
		return tableState{}, nil
	}
	stmt := &gorm.Statement{DB: db, Schema: sc}
	table := tableState{
		Columns:     map[string]columnState{},
		Indexes:     map[string]indexState{},
		ForeignKeys: map[string]foreignKeyState{},
		PrimaryKeys: make([]string, 0),
	}
	for _, field := range sc.Fields {
		if shouldSkipField(field) {
			continue
		}
		definition := normalizeDefinition(exprToString(db.Migrator().FullDataTypeOf(field)))
		if definition == "" {
			continue
		}
		table.Columns[field.DBName] = columnState{Definition: definition}
		if field.PrimaryKey {
			table.PrimaryKeys = append(table.PrimaryKeys, field.DBName)
		}
	}

	parsedIndexes := sc.ParseIndexes()
	if err := validateParsedIndexTags(stmt, parsedIndexes); err != nil {
		return tableState{}, err
	}
	for _, indexName := range sortedKeys(parsedIndexes) {
		if strings.EqualFold(indexName, "PRIMARY") {
			continue
		}
		index := parsedIndexes[indexName]
		if strings.TrimSpace(index.Where) != "" {
			return tableState{}, fmt.Errorf("table `%s` index `%s` uses where=%q, which is unsupported for MySQL migrations", sc.Table, indexName, strings.TrimSpace(index.Where))
		}
		idx := indexState{
			Class:   normalizeIndexClass(index.Class),
			Type:    strings.TrimSpace(index.Type),
			Where:   strings.TrimSpace(index.Where),
			Comment: strings.TrimSpace(index.Comment),
			Option:  strings.TrimSpace(index.Option),
			Fields:  make([]indexFieldState, 0, len(index.Fields)),
		}
		for _, opt := range index.Fields {
			field := indexFieldState{
				Expression: strings.TrimSpace(opt.Expression),
				Sort:       strings.ToUpper(strings.TrimSpace(opt.Sort)),
				Collate:    strings.TrimSpace(opt.Collate),
				Length:     opt.Length,
			}
			if opt.Field != nil {
				field.Column = strings.TrimSpace(opt.Field.DBName)
			}
			if field.Column == "" && field.Expression == "" {
				continue
			}
			idx.Fields = append(idx.Fields, field)
		}
		if len(idx.Fields) == 0 {
			continue
		}
		table.Indexes[indexName] = idx
	}
	sort.Strings(table.PrimaryKeys)
	return table, nil
}

func collectForeignKeysByTable(schemas map[string]*schema.Schema) (map[string]map[string]foreignKeyState, error) {
	result := map[string]map[string]foreignKeyState{}
	signaturesByTable := map[string]map[string]string{}
	for _, tableName := range sortedKeys(schemas) {
		result[tableName] = map[string]foreignKeyState{}
		signaturesByTable[tableName] = map[string]string{}
	}

	var firstErr error
	for _, tableName := range sortedKeys(schemas) {
		sc := schemas[tableName]
		walkRelationships(&sc.Relationships, func(rel *schema.Relationship) {
			if firstErr != nil || rel == nil {
				return
			}
			constraint := rel.ParseConstraint()
			if constraint == nil || constraint.Schema == nil || constraint.ReferenceSchema == nil {
				return
			}
			if strings.TrimSpace(constraint.Schema.Table) == "" {
				return
			}
			fkName := strings.TrimSpace(constraint.Name)
			if fkName == "" {
				firstErr = fmt.Errorf("table `%s` has unnamed foreign key constraint", constraint.Schema.Table)
				return
			}
			fk, err := foreignKeyFromConstraint(constraint)
			if err != nil {
				firstErr = err
				return
			}
			fkMap := result[constraint.Schema.Table]
			if fkMap == nil {
				fkMap = map[string]foreignKeyState{}
				result[constraint.Schema.Table] = fkMap
			}
			signatureMap := signaturesByTable[constraint.Schema.Table]
			if signatureMap == nil {
				signatureMap = map[string]string{}
				signaturesByTable[constraint.Schema.Table] = signatureMap
			}
			normalized := normalizeForeignKey(fk)
			signature := foreignKeySignature(normalized)
			if existingName, exists := signatureMap[signature]; exists {
				if fkName < existingName {
					delete(fkMap, existingName)
					fkMap[fkName] = normalized
					signatureMap[signature] = fkName
				}
				return
			}
			if existing, exists := fkMap[fkName]; exists {
				if !reflect.DeepEqual(normalizeForeignKey(existing), normalized) {
					firstErr = fmt.Errorf("table `%s` has conflicting foreign key definition for `%s`", constraint.Schema.Table, fkName)
				}
				return
			}
			fkMap[fkName] = normalized
			signatureMap[signature] = fkName
		})
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return result, nil
}

func foreignKeyFromConstraint(c *schema.Constraint) (foreignKeyState, error) {
	if c == nil || c.Schema == nil || c.ReferenceSchema == nil {
		return foreignKeyState{}, fmt.Errorf("invalid foreign key constraint")
	}
	if len(c.ForeignKeys) == 0 || len(c.References) == 0 {
		return foreignKeyState{}, fmt.Errorf("table `%s` constraint `%s` has empty key columns", c.Schema.Table, c.Name)
	}
	if len(c.ForeignKeys) != len(c.References) {
		return foreignKeyState{}, fmt.Errorf("table `%s` constraint `%s` has mismatched key columns", c.Schema.Table, c.Name)
	}

	cols := make([]string, 0, len(c.ForeignKeys))
	refCols := make([]string, 0, len(c.References))
	for i := range c.ForeignKeys {
		fkField := c.ForeignKeys[i]
		refField := c.References[i]
		if fkField == nil || refField == nil {
			return foreignKeyState{}, fmt.Errorf("table `%s` constraint `%s` has nil fields", c.Schema.Table, c.Name)
		}
		col := strings.TrimSpace(fkField.DBName)
		refCol := strings.TrimSpace(refField.DBName)
		if col == "" || refCol == "" {
			return foreignKeyState{}, fmt.Errorf("table `%s` constraint `%s` has empty column names", c.Schema.Table, c.Name)
		}
		cols = append(cols, col)
		refCols = append(refCols, refCol)
	}
	return normalizeForeignKey(foreignKeyState{
		Columns:    cols,
		RefTable:   strings.TrimSpace(c.ReferenceSchema.Table),
		RefColumns: refCols,
		OnDelete:   strings.TrimSpace(c.OnDelete),
		OnUpdate:   strings.TrimSpace(c.OnUpdate),
	}), nil
}

func newDryRunMySQL() (*gorm.DB, func(), error) {
	sqlDB, _, err := sqlmock.New()
	if err != nil {
		return nil, nil, err
	}
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy:                           schema.NamingStrategy{},
	})
	if err != nil {
		_ = sqlDB.Close()
		return nil, nil, err
	}
	return db, func() { _ = sqlDB.Close() }, nil
}

func shouldSkipField(field *schema.Field) bool {
	if field == nil {
		return true
	}
	if field.IgnoreMigration {
		return true
	}
	if strings.TrimSpace(field.DBName) == "" {
		return true
	}
	return false
}

func exprToString(expr clause.Expr) string {
	sqlText := expr.SQL
	for _, v := range expr.Vars {
		sqlText = strings.Replace(sqlText, "?", fmt.Sprintf("%v", v), 1)
	}
	return sqlText
}

func normalizeDefinition(definition string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(definition)), " ")
}

func buildDiff(previous, current schemaState) ([]string, []string) {
	ops := make([]migrationOp, 0)

	prevTables := sortedKeys(previous.Tables)
	curTables := sortedKeys(current.Tables)
	prevSet := make(map[string]bool, len(prevTables))
	curSet := make(map[string]bool, len(curTables))
	for _, t := range prevTables {
		prevSet[t] = true
	}
	for _, t := range curTables {
		curSet[t] = true
	}

	for _, tableName := range curTables {
		if !prevSet[tableName] {
			create := createTableSQL(tableName, current.Tables[tableName])
			drop := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)
			ops = append(ops, migrationOp{up: create, down: drop})
		}
	}

	for _, tableName := range curTables {
		if prevSet[tableName] {
			continue
		}
		ops = append(ops, addForeignKeyOpsForNewTable(tableName, current.Tables[tableName])...)
	}

	for _, tableName := range prevTables {
		if !curSet[tableName] {
			ops = append(ops, restoreForeignKeyOpsForDroppedTable(tableName, previous.Tables[tableName])...)
			drop := fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)
			create := createTableSQL(tableName, previous.Tables[tableName])
			ops = append(ops, migrationOp{up: drop, down: create})
		}
	}

	for _, tableName := range curTables {
		if !prevSet[tableName] {
			continue
		}
		ops = append(ops, diffTable(tableName, previous.Tables[tableName], current.Tables[tableName])...)
	}

	up := make([]string, 0, len(ops))
	down := make([]string, 0, len(ops))
	for _, op := range ops {
		if strings.TrimSpace(op.up) != "" {
			up = append(up, op.up)
		}
	}
	for i := len(ops) - 1; i >= 0; i-- {
		if strings.TrimSpace(ops[i].down) != "" {
			down = append(down, ops[i].down)
		}
	}
	return up, down
}

func diffTable(tableName string, prev, cur tableState) []migrationOp {
	ops := make([]migrationOp, 0)
	fkDropOps, fkAddOps := diffForeignKeys(tableName, prev.ForeignKeys, cur.ForeignKeys)
	ops = append(ops, fkDropOps...)

	prevCols := sortedKeys(prev.Columns)
	curCols := sortedKeys(cur.Columns)
	prevSet := make(map[string]bool, len(prevCols))
	curSet := make(map[string]bool, len(curCols))
	for _, c := range prevCols {
		prevSet[c] = true
	}
	for _, c := range curCols {
		curSet[c] = true
	}

	for _, col := range curCols {
		if !prevSet[col] {
			add := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s;", tableName, col, cur.Columns[col].Definition)
			drop := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`;", tableName, col)
			ops = append(ops, migrationOp{up: add, down: drop})
			continue
		}
		if normalizeDefinition(prev.Columns[col].Definition) != normalizeDefinition(cur.Columns[col].Definition) {
			mod := fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` %s;", tableName, col, cur.Columns[col].Definition)
			rollback := fmt.Sprintf("ALTER TABLE `%s` MODIFY COLUMN `%s` %s;", tableName, col, prev.Columns[col].Definition)
			ops = append(ops, migrationOp{up: mod, down: rollback})
		}
	}

	for _, col := range prevCols {
		if !curSet[col] {
			drop := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`;", tableName, col)
			add := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s;", tableName, col, prev.Columns[col].Definition)
			ops = append(ops, migrationOp{up: drop, down: add})
		}
	}

	prevIndexes := sortedKeys(prev.Indexes)
	curIndexes := sortedKeys(cur.Indexes)
	prevIndexSet := make(map[string]bool, len(prevIndexes))
	curIndexSet := make(map[string]bool, len(curIndexes))
	for _, idx := range prevIndexes {
		prevIndexSet[idx] = true
	}
	for _, idx := range curIndexes {
		curIndexSet[idx] = true
	}

	for _, idx := range curIndexes {
		if !prevIndexSet[idx] {
			create := createIndexSQL(tableName, idx, cur.Indexes[idx])
			drop := dropIndexSQL(tableName, idx)
			ops = append(ops, migrationOp{up: create, down: drop})
			continue
		}
		prevIndex := normalizeIndex(prev.Indexes[idx])
		curIndex := normalizeIndex(cur.Indexes[idx])
		if !reflect.DeepEqual(prevIndex, curIndex) {
			up := strings.Join([]string{
				dropIndexSQL(tableName, idx),
				createIndexSQL(tableName, idx, cur.Indexes[idx]),
			}, "\n")
			down := strings.Join([]string{
				dropIndexSQL(tableName, idx),
				createIndexSQL(tableName, idx, prev.Indexes[idx]),
			}, "\n")
			ops = append(ops, migrationOp{up: up, down: down})
		}
	}

	for _, idx := range prevIndexes {
		if !curIndexSet[idx] {
			drop := dropIndexSQL(tableName, idx)
			create := createIndexSQL(tableName, idx, prev.Indexes[idx])
			ops = append(ops, migrationOp{up: drop, down: create})
		}
	}
	ops = append(ops, fkAddOps...)
	return ops
}

func diffForeignKeys(tableName string, prev, cur map[string]foreignKeyState) ([]migrationOp, []migrationOp) {
	dropOps := make([]migrationOp, 0)
	addOps := make([]migrationOp, 0)

	prevNames := sortedKeys(prev)
	curNames := sortedKeys(cur)
	prevSet := make(map[string]bool, len(prevNames))
	curSet := make(map[string]bool, len(curNames))
	for _, name := range prevNames {
		prevSet[name] = true
	}
	for _, name := range curNames {
		curSet[name] = true
	}

	for _, name := range prevNames {
		if !curSet[name] {
			dropOps = append(dropOps, migrationOp{
				up:   dropForeignKeySQL(tableName, name),
				down: createForeignKeySQL(tableName, name, prev[name]),
			})
			continue
		}
		if !reflect.DeepEqual(normalizeForeignKey(prev[name]), normalizeForeignKey(cur[name])) {
			dropOps = append(dropOps, migrationOp{
				up:   dropForeignKeySQL(tableName, name),
				down: createForeignKeySQL(tableName, name, prev[name]),
			})
			addOps = append(addOps, migrationOp{
				up:   createForeignKeySQL(tableName, name, cur[name]),
				down: dropForeignKeySQL(tableName, name),
			})
		}
	}

	for _, name := range curNames {
		if prevSet[name] {
			continue
		}
		addOps = append(addOps, migrationOp{
			up:   createForeignKeySQL(tableName, name, cur[name]),
			down: dropForeignKeySQL(tableName, name),
		})
	}
	return dropOps, addOps
}

func addForeignKeyOpsForNewTable(tableName string, table tableState) []migrationOp {
	names := sortedKeys(table.ForeignKeys)
	ops := make([]migrationOp, 0, len(names))
	for _, name := range names {
		ops = append(ops, migrationOp{
			up:   createForeignKeySQL(tableName, name, table.ForeignKeys[name]),
			down: dropForeignKeySQL(tableName, name),
		})
	}
	return ops
}

func restoreForeignKeyOpsForDroppedTable(tableName string, table tableState) []migrationOp {
	names := sortedKeys(table.ForeignKeys)
	ops := make([]migrationOp, 0, len(names))
	for _, name := range names {
		ops = append(ops, migrationOp{
			up:   "",
			down: createForeignKeySQL(tableName, name, table.ForeignKeys[name]),
		})
	}
	return ops
}

func createTableSQL(tableName string, table tableState) string {
	colNames := sortedKeys(table.Columns)
	defs := make([]string, 0, len(colNames)+1)
	for _, col := range colNames {
		defs = append(defs, fmt.Sprintf("  `%s` %s", col, table.Columns[col].Definition))
	}
	if len(table.PrimaryKeys) > 0 {
		pkCols := make([]string, 0, len(table.PrimaryKeys))
		for _, pk := range table.PrimaryKeys {
			pkCols = append(pkCols, fmt.Sprintf("`%s`", pk))
		}
		defs = append(defs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	indexNames := sortedKeys(table.Indexes)
	for _, indexName := range indexNames {
		defs = append(defs, fmt.Sprintf("  %s", createTableIndexDefinition(indexName, table.Indexes[indexName])))
	}
	return fmt.Sprintf("CREATE TABLE `%s` (\n%s\n);", tableName, strings.Join(defs, ",\n"))
}

func normalizeIndexClass(class string) string {
	return strings.ToUpper(strings.TrimSpace(class))
}

func normalizeIndex(idx indexState) indexState {
	out := indexState{
		Class:   normalizeIndexClass(idx.Class),
		Type:    strings.TrimSpace(idx.Type),
		Where:   strings.TrimSpace(idx.Where),
		Comment: strings.TrimSpace(idx.Comment),
		Option:  strings.TrimSpace(idx.Option),
		Fields:  make([]indexFieldState, 0, len(idx.Fields)),
	}
	for _, f := range idx.Fields {
		out.Fields = append(out.Fields, indexFieldState{
			Column:     strings.TrimSpace(f.Column),
			Expression: strings.TrimSpace(f.Expression),
			Sort:       strings.ToUpper(strings.TrimSpace(f.Sort)),
			Collate:    strings.TrimSpace(f.Collate),
			Length:     f.Length,
		})
	}
	return out
}

func createIndexSQL(tableName, indexName string, idx indexState) string {
	idx = normalizeIndex(idx)
	classPrefix := indexClassPrefix(idx.Class)
	sql := fmt.Sprintf("CREATE %sINDEX `%s` ON `%s` (%s)", classPrefix, indexName, tableName, indexFieldsSQL(idx.Fields))
	if idx.Type != "" {
		sql += " USING " + idx.Type
	}
	if idx.Comment != "" {
		sql += " COMMENT " + quoteSQLString(idx.Comment)
	}
	if idx.Option != "" {
		sql += " " + idx.Option
	}
	return sql + ";"
}

func dropIndexSQL(tableName, indexName string) string {
	return fmt.Sprintf("DROP INDEX `%s` ON `%s`;", indexName, tableName)
}

func createTableIndexDefinition(indexName string, idx indexState) string {
	idx = normalizeIndex(idx)
	keyPrefix := indexClassKeyPrefix(idx.Class)
	definition := fmt.Sprintf("%s `%s` (%s)", keyPrefix, indexName, indexFieldsSQL(idx.Fields))
	if idx.Comment != "" {
		definition += " COMMENT " + quoteSQLString(idx.Comment)
	}
	if idx.Option != "" {
		definition += " " + idx.Option
	}
	return definition
}

func indexClassPrefix(class string) string {
	switch normalizeIndexClass(class) {
	case "UNIQUE":
		return "UNIQUE "
	case "FULLTEXT":
		return "FULLTEXT "
	case "SPATIAL":
		return "SPATIAL "
	default:
		return ""
	}
}

func indexClassKeyPrefix(class string) string {
	switch normalizeIndexClass(class) {
	case "UNIQUE":
		return "UNIQUE KEY"
	case "FULLTEXT":
		return "FULLTEXT KEY"
	case "SPATIAL":
		return "SPATIAL KEY"
	default:
		return "KEY"
	}
}

func indexFieldsSQL(fields []indexFieldState) string {
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, indexFieldSQL(field))
	}
	return strings.Join(parts, ", ")
}

func indexFieldSQL(field indexFieldState) string {
	var base string
	if strings.TrimSpace(field.Expression) != "" {
		base = strings.TrimSpace(field.Expression)
	} else {
		base = fmt.Sprintf("`%s`", strings.TrimSpace(field.Column))
		if field.Length > 0 {
			base = fmt.Sprintf("%s(%d)", base, field.Length)
		}
		if strings.TrimSpace(field.Collate) != "" {
			base = fmt.Sprintf("%s COLLATE %s", base, strings.TrimSpace(field.Collate))
		}
	}
	if strings.TrimSpace(field.Sort) != "" {
		base = fmt.Sprintf("%s %s", base, strings.ToUpper(strings.TrimSpace(field.Sort)))
	}
	return base
}

func normalizeForeignKey(fk foreignKeyState) foreignKeyState {
	out := foreignKeyState{
		Columns:    make([]string, 0, len(fk.Columns)),
		RefTable:   strings.TrimSpace(fk.RefTable),
		RefColumns: make([]string, 0, len(fk.RefColumns)),
		OnDelete:   normalizeForeignKeyAction(fk.OnDelete),
		OnUpdate:   normalizeForeignKeyAction(fk.OnUpdate),
	}
	for _, col := range fk.Columns {
		col = strings.TrimSpace(col)
		if col != "" {
			out.Columns = append(out.Columns, col)
		}
	}
	for _, col := range fk.RefColumns {
		col = strings.TrimSpace(col)
		if col != "" {
			out.RefColumns = append(out.RefColumns, col)
		}
	}
	return out
}

func foreignKeySignature(fk foreignKeyState) string {
	fk = normalizeForeignKey(fk)
	return strings.Join([]string{
		strings.Join(fk.Columns, ","),
		fk.RefTable,
		strings.Join(fk.RefColumns, ","),
		fk.OnDelete,
		fk.OnUpdate,
	}, "|")
}

func normalizeForeignKeyAction(action string) string {
	return strings.ToUpper(strings.TrimSpace(action))
}

func createForeignKeySQL(tableName, constraintName string, fk foreignKeyState) string {
	fk = normalizeForeignKey(fk)
	parts := []string{
		fmt.Sprintf("ALTER TABLE `%s` ADD CONSTRAINT `%s`", tableName, constraintName),
		fmt.Sprintf("FOREIGN KEY (%s)", quotedColumns(fk.Columns)),
		fmt.Sprintf("REFERENCES `%s` (%s)", fk.RefTable, quotedColumns(fk.RefColumns)),
	}
	if fk.OnDelete != "" {
		parts = append(parts, "ON DELETE "+fk.OnDelete)
	}
	if fk.OnUpdate != "" {
		parts = append(parts, "ON UPDATE "+fk.OnUpdate)
	}
	return strings.Join(parts, " ") + ";"
}

func dropForeignKeySQL(tableName, constraintName string) string {
	return fmt.Sprintf("ALTER TABLE `%s` DROP FOREIGN KEY `%s`;", tableName, constraintName)
}

func quotedColumns(columns []string) string {
	parts := make([]string, 0, len(columns))
	for _, col := range columns {
		parts = append(parts, fmt.Sprintf("`%s`", strings.TrimSpace(col)))
	}
	return strings.Join(parts, ", ")
}

func quoteSQLString(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func validateParsedIndexTags(stmt *gorm.Statement, parsed map[string]schema.Index) error {
	if stmt == nil || stmt.Schema == nil || stmt.DB == nil || stmt.DB.Config == nil {
		return nil
	}
	namer := stmt.DB.Config.NamingStrategy
	if namer == nil {
		namer = schema.NamingStrategy{}
	}
	issues := make([]string, 0)
	for _, field := range stmt.Schema.Fields {
		if field == nil {
			continue
		}
		decls, err := parseFieldIndexTagDecls(stmt.Schema.Table, field, namer)
		if err != nil {
			return err
		}
		for _, decl := range decls {
			index, ok := parsed[decl.Name]
			if !ok {
				issues = append(issues, fmt.Sprintf("%s.%s tag `%s` expected index `%s` but it was not parsed", stmt.Schema.Table, field.Name, decl.Raw, decl.Name))
				continue
			}
			if !indexContainsDecl(index, decl) {
				issues = append(issues, fmt.Sprintf("%s.%s tag `%s` expected column/expression in index `%s` but it was not found", stmt.Schema.Table, field.Name, decl.Raw, decl.Name))
			}
		}
	}
	if len(issues) == 0 {
		return nil
	}
	sort.Strings(issues)
	return fmt.Errorf("failed to parse gorm index tags:\n- %s", strings.Join(issues, "\n- "))
}

func parseFieldIndexTagDecls(tableName string, field *schema.Field, namer schema.Namer) ([]indexTagDecl, error) {
	if field == nil {
		return nil, nil
	}
	rawTag := field.Tag.Get("gorm")
	if strings.TrimSpace(rawTag) == "" {
		return nil, nil
	}
	decls := make([]indexTagDecl, 0)
	for _, value := range strings.Split(rawTag, ";") {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		parts := strings.Split(value, ":")
		key := strings.TrimSpace(strings.ToUpper(parts[0]))
		if key != "INDEX" && key != "UNIQUEINDEX" {
			continue
		}

		tag := strings.Join(parts[1:], ":")
		idx := strings.Index(tag, ",")
		if idx == -1 {
			idx = len(tag)
		}
		name := strings.TrimSpace(tag[:idx])
		tagSetting := strings.Join(strings.Split(tag, ",")[1:], ",")
		settings := schema.ParseTagSetting(tagSetting, ",")

		if name == "" {
			subName := field.Name
			const compositeTag = "COMPOSITE"
			if composite, found := settings[compositeTag]; found {
				if len(composite) == 0 || composite == compositeTag {
					return nil, fmt.Errorf("invalid empty composite index tag on %s.%s", tableName, field.Name)
				}
				subName = composite
			}
			name = namer.IndexName(tableName, subName)
		}

		decls = append(decls, indexTagDecl{
			Name:       name,
			Column:     strings.TrimSpace(field.DBName),
			Expression: strings.TrimSpace(settings["EXPRESSION"]),
			Raw:        value,
		})
	}
	return decls, nil
}

func indexContainsDecl(index schema.Index, decl indexTagDecl) bool {
	expectedExpression := strings.TrimSpace(decl.Expression)
	expectedColumn := strings.TrimSpace(decl.Column)
	for _, opt := range index.Fields {
		if expectedExpression != "" {
			if strings.TrimSpace(opt.Expression) == expectedExpression {
				return true
			}
			continue
		}
		if opt.Field != nil && strings.TrimSpace(opt.Field.DBName) == expectedColumn {
			return true
		}
	}
	return false
}

func sanitizeName(raw string) string {
	name := strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	s := strings.Trim(b.String(), "_")
	if s == "" {
		return "auto_migration"
	}
	return s
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
