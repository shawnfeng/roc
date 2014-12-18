package jobs

import (
	"time"
)

type jobCmdType int32
const (
	_                        = iota
	JOBCMD_START  jobCmdType = iota
	JOBCMD_STOP
	JOBCMD_KILL
	// 更新配置
	JOBCMD_UPCONF

	// job移除
	JOBCMD_REMOVE

)

func (m jobCmdType) String() string {
	s := "UNKNOWN"

	switch m {
	case JOBCMD_START:
		s = "JOBCMD_START"
	case JOBCMD_STOP:
		s = "JOBCMD_STOP"
	case JOBCMD_KILL:
		s = "JOBCMD_KILL"
	case JOBCMD_UPCONF:
		s = "JOBCMD_UPCONF"

	case JOBCMD_REMOVE:
		s = "JOBCMD_REMOVE"

	}


	return s
}


type jobExitType int32
const (
	_                     = iota
	Exit_OK   jobExitType = iota
	Exit_Err_StdoutPipe
	Exit_Err_CmdStart

	Exit_Err_Kill
	Exit_Err_JobOtherErr

)



func (m jobExitType) String() string {
	s := "UNKNOWN"

	switch m {
	case Exit_OK:
		s = "OK"

	case Exit_Err_StdoutPipe:
		s = "Err_StdoutPipe"
	case Exit_Err_CmdStart:
		s = "Err_CmdStart"

	case Exit_Err_Kill:
		s = "Err_Kill"

	case Exit_Err_JobOtherErr:
		s = "Err_JobOtherErr"
	}

	return s
}

type runNotify struct {
	jobPid int
}

type exitState struct {
	runDuration time.Duration
	exitType jobExitType
	runErr error
}


type userCommand struct {

	cmd jobCmdType
	// callback
}





