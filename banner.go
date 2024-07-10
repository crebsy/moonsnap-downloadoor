package main

import (
	"github.com/mbndr/figlet4go"
)

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
	ascii.LoadFont(".")
	renderStr, _ := ascii.RenderOpts("moonsnap", options)
	return renderStr
}
