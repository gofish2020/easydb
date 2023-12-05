package utils

import "github.com/kardianos/osext"

// ExecDir 当前可执行程序目录
func ExecDir() string {

	path, err := osext.ExecutableFolder()
	if err != nil {
		return ""
	}
	return path
}
