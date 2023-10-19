package migrator

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	dm "github.com/shopmonkeyus/eds-server/internal/model"
)

const spacer = "    "

func WriteChangeHeader(w io.Writer, name string, typename string) {
	var buf = bufio.NewWriter(w)
	buf.WriteString(fmt.Sprintf("[*] Changing the `%s` %s\n", name, typename))
	buf.Flush()
}

func WriteRemovedHeader(w io.Writer, name string, typename string) {
	var buf = bufio.NewWriter(w)
	buf.WriteString(fmt.Sprintf("[-] Removing the `%s` %s\n", name, typename))
	buf.Flush()
}

func WriteAddedHeader(w io.Writer, name string, typename string, extra ...string) {
	var buf = bufio.NewWriter(w)
	detail := strings.Join(extra, " ")
	buf.WriteString(fmt.Sprintf("[+] Adding the `%s` %s %s\n", name, typename, detail))
	buf.Flush()
}

func WriteAddedLine(w io.Writer, typename string, value string, extra ...string) {
	var buf = bufio.NewWriter(w)
	detail := strings.Join(extra, " ")
	buf.WriteString(fmt.Sprintf("    [+] Adding the %s `%s` %s\n", typename, value, detail))
	buf.Flush()
}

func WriteRemovedLine(w io.Writer, typename string, value string, extra ...string) {
	var buf = bufio.NewWriter(w)
	detail := strings.Join(extra, " ")
	buf.WriteString(fmt.Sprintf("    [-] Removing the %s `%s` %s\n", typename, value, detail))
	buf.Flush()
}

func WriteAlterLine(w io.Writer, typename string, value string, extra ...string) {
	var buf = bufio.NewWriter(w)
	detail := strings.Join(extra, " ")
	buf.WriteString(fmt.Sprintf("    [*] Altering the %s `%s` %s\n", typename, value, detail))
	buf.Flush()
}

func findField(model *dm.Model, column string) *dm.Field {
	for _, field := range model.Fields {
		if field.Name == column {
			return field
		}
	}
	return nil
}

func toPrismaType(datatype string, userDefinedtype *string, isnullable bool) string {
	if datatype == "USER-DEFINED" {
		rel := *userDefinedtype
		if isnullable {
			rel += "?"
		}
		return rel
	}
	if userDefinedtype != nil && *userDefinedtype == "_text" {
		return "String[]"
	}
	var dt string
	switch datatype {
	case "text", "character varying":
		dt = "String"
	case "jsonb":
		dt = "Json"
	case "timestamp with time zone", "timestamp without time zone":
		dt = "DateTime"
	case "bytes":
		dt = "Bytes"
	case "bigint":
		dt = "Int"
	case "integer", "numeric":
		dt = "Int"
	case "boolean":
		dt = "Boolean"
	case "double precision":
		dt = "Float"
	case "ARRAY":
		if userDefinedtype != nil && *userDefinedtype == "_timestamptz" {
			return "DateTime[]"
		}
		if userDefinedtype != nil && strings.HasPrefix(*userDefinedtype, "_") {
			return (*userDefinedtype)[1:] + "[]"
		}
		dt = datatype
	default:
		dt = datatype
	}
	if isnullable {
		dt += "?"
	}
	return dt

}
