package util

import "github.com/fatih/color"

var green = color.New(color.FgGreen).SprintFunc()
var whiteBold = color.New(color.FgWhite, color.Bold).SprintFunc()

func GenerateHelpSection(title string, body string) string {
	return green(title) + "\n\n" + whiteBold(body)
}
