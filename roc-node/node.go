// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"os"

	"github.com/shawnfeng/roc/roc-node/engine"
)

func main() {
	engine.Power(os.Args[1])
}
