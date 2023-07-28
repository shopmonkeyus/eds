package util

import (
	"encoding/json"

	"regexp"
	"sort"
	"strings"
)

func QuoteJoin(vals []string, quote string, sep string) string {
	res := make([]string, 0)
	for _, val := range vals {
		res = append(res, quote+val+quote)
	}
	return strings.Join(res, sep)
}

func SafeCompare(a []string, b []string) bool {
	_a := make([]string, len(a))
	_b := make([]string, len(b))
	copy(_a, a)
	copy(_b, b)
	sort.Strings(_a)
	sort.Strings(_b)
	return strings.Join(_a, "") == strings.Join(_b, "")
}

func Exists(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}

func EndsWithSuffix(val string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(val, suffix) {
			return true
		}
	}
	return false
}

func Dequote(val string) string {
	return Trim(Trim(strings.ReplaceAll(val, `\"`, `"`), `"`, `"`), "'", "'")
}

func Trim(val string, prefix string, suffix string) string {
	if strings.HasPrefix(val, prefix) && strings.HasSuffix(val, suffix) {
		return val[len(prefix) : len(val)-len(suffix)]
	}
	return val
}

func Tokenize(val string) []string {
	tokens := strings.Split(val, ",")
	results := make([]string, 0)
	for _, token := range tokens {
		results = append(results, strings.TrimSpace(token))
	}
	return results
}

func TokenizeWrap(tokens []string, prefix string, suffix string) []string {
	results := make([]string, 0)
	for _, token := range tokens {
		results = append(results, prefix+strings.TrimSpace(token)+suffix)
	}
	return results
}

func AddIfNotExists(val string, slice *[]string) bool {
	for _, line := range *slice {
		if line == val {
			return false
		}
	}
	*slice = append(*slice, val)
	return true
}

var ctre = regexp.MustCompile(`(_[a-z])`)

func CamelToTitleCase(val string) string {
	val = strings.ToLower(val)
	val = ctre.ReplaceAllStringFunc(val, func(group string) string {
		return strings.ReplaceAll(strings.ToUpper(group), "_", " ")
	})
	return strings.ToUpper(val[0:1]) + val[1:]
}

func MustJSONStringify(val interface{}, pretty bool) string {
	if pretty {
		buf, err := json.MarshalIndent(val, "", " ")
		if err != nil {
			panic(err)
		}
		return string(buf)
	}
	buf, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(buf)
}

func Dedupe(vals []string) []string {
	dedupe := make(map[string]bool)
	results := make([]string, 0)
	// maintain the original order, just removing the duplicates
	for _, val := range vals {
		if !dedupe[val] {
			results = append(results, val)
			dedupe[val] = true
		}
	}
	return results
}

// var treeConfig = tree.PrettyCfg{
// 	LineWidth:      80,
// 	TabWidth:       2,
// 	UseTabs:        false,
// 	Simplify:       true,
// 	JSONFmt:        true,
// 	ValueRedaction: false,
// 	Align:          tree.PrettyNoAlign,
// 	Case:           strings.ToUpper,
// }

// func FormatSQLExpr(sql string) (string, error) {
// 	expr, err := parser.ParseExpr(sql)
// 	if err != nil {
// 		return "", err
// 	}
// 	parsed := strings.TrimSpace(treeConfig.Pretty(expr))
// 	return parsed, nil
// }
