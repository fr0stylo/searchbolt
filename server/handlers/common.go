package handlers

import (
	"github.com/gorilla/schema"
	"github.com/unrolled/render"
)


var decoder = schema.NewDecoder()
var r = render.New(render.Options{
	StreamingJSON: true,
})
