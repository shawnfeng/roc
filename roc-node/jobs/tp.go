// Copyright 2014 The roc Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


package jobs


type jobRunCtrl int32
const (
	_                         = iota
	RUNCTRL_START  jobRunCtrl = iota
	RUNCTRL_STOP

	RUNCTRL_KILL
	// job移除
	RUNCTRL_REMOVE

)

func (m jobRunCtrl) String() string {
	s := "RUNCTRL_UNKNOWN"

	switch m {
	case RUNCTRL_START:
		s = "RUNCTRL_START"
	case RUNCTRL_STOP:
		s = "RUNCTRL_STOP"

	case RUNCTRL_KILL:
		s = "RUNCTRL_KILL"

	case RUNCTRL_REMOVE:
		s = "RUNCTRL_REMOVE"

	}


	return s
}







