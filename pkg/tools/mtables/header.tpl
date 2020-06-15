package {{$.Package}}

import(
{{range $k, $v:= $.Imports}}
    {{$k}} "{{$v}}"
{{end}}
)

func noop(_ interface{}){}