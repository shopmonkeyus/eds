package migrator

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mitchellh/colorstring"
	"github.com/schollz/progressbar/v3"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

type MigrateOpts struct {
	DryRun     bool
	Format     string
	DBName     string
	NoProgress bool
	NoConfirm  bool
	Quiet      bool
	ShowSQL    bool
	SkipCreate bool
}

func loadTableSchema(logger logger.Logger, db *pgxpool.Pool, tableName string) ([]Column, error) {
	started := time.Now()
	query := `SELECT
	c.table_name,
	c.column_name,
	c.column_default,
	c.is_nullable,
	c.data_type,
	c.character_maximum_length
FROM
	information_schema.columns c
WHERE
	c.table_schema = 'public' AND
	c.table_name = '@tableName'
ORDER BY
	c.table_name, c.ordinal_position;`
	rows, err := db.Query(context.TODO(), query, pgx.NamedArgs{"tableName": tableName})
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			// this means there is not a table made and we need to build one...
			return []Column{}, nil
		}
		return nil, fmt.Errorf("error fetching column metadata from the db: %w", err)
	}
	defer rows.Close()

	var table string
	var columns []Column
	for rows.Next() {
		var tn string
		var cn string
		var ihd string
		var cd sql.NullString
		var isn string
		var dt string
		var cml sql.NullString

		if err := rows.Scan(&tn, &cn, &cd, &ihd, &isn, &dt, &cml); err != nil {
			return nil, fmt.Errorf("error reading db row: %w", err)
		}
		if table != tn {
			columns = make([]Column, 0)
			table = tn
		}
		var colDef, maxlength, udtName *string
		if cd.Valid {
			colDef = &cd.String
		}
		if cml.Valid {
			maxlength = &cml.String
		}

		columns = append(columns, Column{
			Table:               table,
			Name:                cn,
			Default:             colDef,
			IsHidden:            ihd == "YES",
			IsNullable:          isn == "YES",
			DataType:            dt,
			MaxLength:           maxlength,
			UserDefinedTypeName: udtName,
			// CRDBType:            crd,
			// Expression:          genExpr,
		})
	}
	logger.Trace("loaded up schema in %v", time.Since(started))
	return columns, nil
}

type sqlWriter struct {
	sql     []string
	buf     bytes.Buffer
	showsql bool
}

var _ io.Writer = (*sqlWriter)(nil)

func (w *sqlWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

var traceSQL bool
var multiSpaceRegexp = regexp.MustCompile(`\s{2,}`)

func (w *sqlWriter) runSQL(pb *progressbar.ProgressBar, logger logger.Logger, db *pgxpool.Pool, sql string, offset int, total int) error {
	if sql == "" || sql == ";" || sql == "\n" {
		return nil
	}
	started := time.Now()
	if _, err := db.Exec(context.TODO(), sql); err != nil {
		if pb != nil {
			pb.Clear()
			pb.Close()
		}
		logger.Error("error executing: %s. %s", sql, err)
		return err
	}
	msg := strings.TrimSpace(strings.ReplaceAll(sql, "\n", " "))
	smsg := multiSpaceRegexp.ReplaceAllString(msg, " ")
	if len(smsg) > 70 {
		smsg = strings.TrimSpace(smsg[0:70])
	}
	if pb != nil {
		pb.Describe(fmt.Sprintf("[magenta][%d/%d][reset] %s", offset, total, smsg))
	}
	if traceSQL {
		logger.Trace("executed: %s, took: %v", msg, time.Since(started))
	}
	if w.showsql {
		colorstring.Fprintf(os.Stderr, "[magenta][%3d/%3d] [light_cyan]%-72s[light_green]%v[reset]\n", offset, total, smsg, time.Since(started).Round(time.Millisecond))
	}
	return nil
}

func (w *sqlWriter) run(logger logger.Logger, db *pgxpool.Pool) error {
	for _, buf := range strings.Split(w.buf.String(), ";") {
		sql := strings.TrimSpace(strings.ReplaceAll(buf, "\n", " "))
		if sql != "" {
			w.sql = append(w.sql, sql)
		}
	}

	total := len(w.sql)

	var bar *progressbar.ProgressBar
	// if !opts.NoProgress {
	// 	ansi.CursorHide()
	// 	bar = progressbar.NewOptions(total,
	// 		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
	// 		progressbar.OptionEnableColorCodes(true),
	// 		progressbar.OptionShowBytes(false),
	// 		progressbar.OptionSetWidth(35),
	// 		progressbar.OptionUseANSICodes(true),
	// 		progressbar.OptionSetPredictTime(true),
	// 		progressbar.OptionSetDescription(fmt.Sprintf("[magenta][0/%d][reset] Starting...", len(w.sql))),
	// 		progressbar.OptionShowDescriptionAtLineEnd(),
	// 		progressbar.OptionSetRenderBlankState(true),
	// 		progressbar.OptionClearOnFinish(),
	// 		progressbar.OptionOnCompletion(func() {
	// 			ansi.CursorShow()
	// 			ansi.EraseInLine(3)
	// 		}),
	// 		progressbar.OptionSetTheme(progressbar.Theme{
	// 			Saucer:        "-",
	// 			AltSaucerHead: "[yellow]<[reset]",
	// 			SaucerHead:    "[yellow]-[reset]",
	// 			SaucerPadding: "[white]•",
	// 			BarStart:      "[blue]|[reset]",
	// 			BarEnd:        "[blue]|[reset]",
	// 		}))
	// 	defer bar.Close()
	// }

	var offset int
	for _, sql := range w.sql {
		if err := w.runSQL(bar, logger, db, sql, offset, total); err != nil {
			return err
		}
		offset++
		if bar != nil {
			bar.Add(1)
		}
	}

	return nil
}

// Migrate will run migration using model against db
func MigrateTable(logger logger.Logger, db *pgxpool.Pool, datamodel *dm.Model, tableName string) error {

	schema, err := loadTableSchema(logger, db, tableName)
	if err != nil {
		return err
	}

	stdout := bufio.NewWriter(os.Stdout)

	var output sqlWriter
	output.showsql = true

	// model diff
	_, modelDiff, err := diffModels(schema, datamodel)
	if err != nil {
		return err
	}

	newTables := make(map[string]bool)

	change := modelDiff
	if change.Action == AddAction {
		newTables[tableName] = true
	}
	// if !opts.Quiet {
	// 	change.Format(tableName, "sql", stdout)
	// }
	change.Format(tableName, "sql", &output)

	stdout.Flush()

	// if output.buf.Len() == 0 {
	// 	if !opts.Quiet {
	// 		logger.Info("no changes detected")
	// 	}
	// 	return nil
	// }

	// if !opts.DryRun {
	// 	if !opts.NoConfirm {
	// 		fmt.Print("Apply changes [y/N]? ")
	// 		reader := bufio.NewReader(os.Stdin)
	// 		ru, _, err := reader.ReadRune()
	// 		if err == io.EOF {
	// 			return nil
	// 		}
	// 		if unicode.ToLower(ru) != 'y' {
	// 			fmt.Println("Cancelled!")
	// 			return nil
	// 		}
	// 	}
	started := time.Now()
	logger.Info("running migrations ...")
	if err := output.run(logger, db); err != nil {
		return err
	}
	logger.Info("executed %d sql statements in %v", len(output.sql), time.Since(started))
	// }
	return nil
}
