package main

import (
	"embed"

	"github.com/mbndr/figlet4go"
)

//go:embed fonts/block.flf
var embedFS embed.FS

func getBanner() string {
	ascii := figlet4go.NewAsciiRender()

	tc, err := figlet4go.NewTrueColorFromHexString("885DBA")
	if err != nil {
		panic(err)
	}
	// Adding the colors to RenderOptions
	options := figlet4go.NewRenderOptions()
	options.FontColor = []figlet4go.Color{
		tc,
	}
	options.FontName = "block"
	fontBytes, err := embedFS.ReadFile("fonts/block.flf")
	if err != nil {
		panic(err)
	}
	ascii.LoadBindataFont(fontBytes, "block")
	renderStr, _ := ascii.RenderOpts("moonsnap", options)
	return renderStr
}
